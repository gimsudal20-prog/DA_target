# -*- coding: utf-8 -*-
from __future__ import annotations
import base64
import copy
import csv
import hashlib
import hmac
import io
import json
import os
import re
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Tuple, Optional
from urllib.parse import urlparse, urlencode
import pandas as pd
import requests
from flask import Flask, Response, jsonify, render_template, request, send_file
from utils.excel import (
    XLSX_MIME,
    build_report_workbook,
    build_table_workbook,
    workbook_to_bytesio,
)
from services.naver_client import (
    build_open_headers,
    create_naver_session,
    make_fake_response,
    make_signature,
    request_naver_api,
)
from services.lookup_service import (
    LookupService,
    normalize_adgroup_item,
    normalize_campaign_item,
    normalize_channel_item,
    pc_mobile_label,
    extract_pc_mobile_flags,
    stable_cache_key,
)
from routes.lookup_routes import create_lookup_blueprint
from routes.detail_lookup_routes import create_detail_lookup_blueprint
from services.detail_lookup_service import DetailLookupService
from routes.account_lookup_routes import create_account_lookup_blueprint
from services.account_lookup_service import AccountLookupService
from routes.registration_routes import create_registration_blueprint
from services.registration_service import RegistrationService
from routes.change_routes import create_change_blueprint
from services.change_service import ChangeService
from routes.copy_delete_routes import create_copy_delete_blueprint
from services.copy_delete_service import CopyDeleteService
from werkzeug.exceptions import HTTPException
from utils.labels import (
    AD_EXTENSION_TYPE_LABELS,
    AD_TYPE_LABELS,
    ADGROUP_TYPE_LABELS,
    CAMPAIGN_TYPE_COLORS,
    CAMPAIGN_TYPE_LABELS,
    format_asset_lookup_excel_value,
    label_ad_type,
    label_adgroup_type,
    label_campaign_type,
    label_extension_type,
)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
SAMPLES_DIR = os.path.join(BASE_DIR, "samples")
OPENAPI_BASE_URL = "https://api.searchad.naver.com"
PATCH_VERSION = "bulk-delete-extension-types-v20-20260429"
app = Flask(__name__, template_folder=TEMPLATES_DIR)
HTTP_SESSION = create_naver_session(pool_connections=32, pool_maxsize=64)
_LOOKUP_SERVICE = LookupService(lambda method, api_key, secret_key, cid, uri, params=None, json_body=None, max_retries=3: request_naver_api(
    method,
    api_key,
    secret_key,
    cid,
    uri,
    params=params,
    json_body=json_body,
    max_retries=max_retries,
    session=HTTP_SESSION,
    base_url=OPENAPI_BASE_URL,
))
# Backward-compatible aliases used by existing helper names and route code.
_CACHE_LOCK = _LOOKUP_SERVICE.cache._lock
_CACHE_TTL_SECONDS = _LOOKUP_SERVICE.cache.ttl_seconds
_CAMPAIGN_CACHE = _LOOKUP_SERVICE.cache.campaigns
_ADGROUP_CACHE = _LOOKUP_SERVICE.cache.adgroups
_CHANNEL_CACHE = _LOOKUP_SERVICE.cache.channels
FAST_IO_WORKERS = 8
DELETE_IO_WORKERS = 12
BID_IO_WORKERS = 8
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
ACTION_LOG_PATH = os.path.join(LOG_DIR, "action_history.jsonl")
_ACTION_LOG_LOCK = threading.RLock()
_ACTION_LOG_MAX_LINES = 2000
def _stable_cache_key(api_key: str, secret_key: str, cid: str, scope: str) -> str:
    return stable_cache_key(api_key, secret_key, cid, scope)
def _cache_get(store: Dict[str, Tuple[float, Any]], key: str, ttl: float = _CACHE_TTL_SECONDS):
    return _LOOKUP_SERVICE.cache.get(store, key, ttl)
def _cache_set(store: Dict[str, Tuple[float, Any]], key: str, value: Any):
    return _LOOKUP_SERVICE.cache.set(store, key, value)
def _cache_invalidate(api_key: str, secret_key: str, cid: str):
    return _LOOKUP_SERVICE.cache.invalidate_account(api_key, secret_key, cid)
LOG_ACTION_LABELS = {
    "/create_campaign": "캠페인 생성",
    "/create_adgroup_simple": "광고그룹 생성",
    "/create_keywords_simple": "키워드 등록",
    "/bulk_upload_text_ads": "텍스트 소재 일괄 업로드",
    "/create_text_ad_simple": "텍스트 소재 생성",
    "/create_ad_advanced": "고급 소재 생성",
    "/create_extension_simple": "확장소재 등록",
    "/bulk_upload_headlines": "추가제목 일괄 업로드",
    "/create_shopping_ad_simple": "쇼핑 소재 생성",
    "/create_extension_raw": "확장소재 원본 등록",
    "/create_restricted_keywords_simple": "제외키워드 등록",
    "/copy_entities_to_adgroups": "엔티티 복사",
    "/copy_campaigns": "캠페인 복사",
    "/copy_adgroups_to_target": "광고그룹 복사",
    "/rename_adgroups_bulk": "광고그룹명 일괄 변경",
    "/update_media": "매체 설정 변경",
    "/update_adgroup_options": "광고그룹 옵션 변경",
    "/update_adgroup_search_options": "파워링크 검색옵션 변경",
    "/update_budget": "예산 변경",
    "/update_schedule": "시간대 설정 변경",
    "/update_schedule_campaign_bulk": "캠페인 시간대 일괄 변경",
    "/update_non_search_keyword_exclusion": "비검색영역 노출제외 변경",
    "/update_keyword_bids": "키워드 입찰가 변경",
    "/update_bid_mode_by_scope": "입찰가 방식 변경",
    "/search_powerlink_keywords": "검색어 기준 키워드 조회",
    "/export_powerlink_keywords_excel": "검색어 기준 키워드 엑셀 다운로드",
    "/export_powerlink_duplicate_keywords_excel": "중복 키워드 엑셀 다운로드",
    "/query_account_keywords": "계정 키워드 조회",
    "/export_account_keywords_excel": "계정 키워드 엑셀 다운로드",
    "/query_account_ads": "계정 소재 조회",
    "/query_account_extensions": "계정 확장소재 조회",
    "/export_account_ads_excel": "계정 소재 엑셀 다운로드",
    "/export_account_extensions_excel": "계정 확장소재 엑셀 다운로드",
    "/update_keyword_bids_by_search": "검색어 기준 키워드 입찰가 변경",
    "/update_keyword_bid_weights_by_search": "검색어 기준 키워드 입찰가중치 변경",
    "/update_powerlink_device_bid_weights": "파워링크 PC/모바일 입찰가중치 변경",
    "/set_searched_powerlink_keyword_state": "조회 키워드 ON/OFF 변경",
    "/adjust_keyword_bids_by_threshold": "상하한 기준 입찰가 조정",
    "/update_keyword_bids_avg_position": "평균순위 기준 입찰가 적용",
    "/preview_keyword_avg_position_by_search": "검색어 기준 평균순위 입찰가 미리보기",
    "/update_keyword_avg_position_by_search": "검색어 기준 평균순위 입찰가 적용",
    "/bulk_delete_by_parent": "부모 기준 일괄 삭제",
    "/bulk_register": "일괄 등록",
    "/bulk_delete": "일괄 삭제",
    "/set_campaign_state": "캠페인 상태 변경",
    "/delete_selected": "선택 삭제",
    "/clear_action_logs": "로그 비우기",
}
ACTION_LOG_EXCLUDED_PATHS = {
    "/get_campaigns", "/get_adgroups", "/get_biz_channels", "/get_keywords", "/get_ads",
    "/get_ad_extensions", "/get_restricted_keywords", "/find_powerlink_duplicate_keywords", "/find_account_powerlink_duplicate_keywords", "/get_powerlink_keyword_stats", "/search_powerlink_keywords", "/export_powerlink_keywords_excel", "/query_account_ads", "/query_account_extensions", "/query_account_keywords", "/export_account_ads_excel", "/export_account_extensions_excel", "/export_account_keywords_excel", "/get_action_logs", "/health", "/favicon.ico", "/",
    "/sample_headers", "/delete_sample_headers", "/clear_action_logs",
}
def _safe_json_body() -> Dict[str, Any]:
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data
    if request.form:
        out: Dict[str, Any] = {}
        for k in request.form.keys():
            vals = request.form.getlist(k)
            out[k] = vals if len(vals) > 1 else (vals[0] if vals else "")
        return out
    return {}
def _line_count(text_value: Any) -> int:
    return len([x for x in str(text_value or "").replace(",", "\n").splitlines() if x.strip()])
def _action_summary(path: str, payload: Dict[str, Any]) -> str:
    if not payload:
        return ""
    if path == "/create_campaign":
        return f"이름={str(payload.get('name') or '').strip()}"
    if path == "/create_adgroup_simple":
        return f"이름={str(payload.get('name') or '').strip()} | 캠페인={str(payload.get('campaign_id') or '').strip()}"
    if path == "/create_keywords_simple":
        if isinstance(payload.get('keyword_batches'), list):
            batches = payload.get('keyword_batches') or []
            slot_cnt = sum(1 for x in batches if str((x or {}).get('adgroup_id') or '').strip() and str((x or {}).get('keywords_text') or '').strip())
            kw_cnt = sum(_line_count((x or {}).get('keywords_text')) for x in batches)
            return f"슬롯={slot_cnt} | 키워드={kw_cnt}"
        return f"키워드={_line_count(payload.get('keywords_text'))}"
    if path in {"/copy_campaigns", "/copy_adgroups_to_target", "/copy_entities_to_adgroups"}:
        src = payload.get('source_ids') or payload.get('entity_ids') or []
        if not isinstance(src, list):
            src = [src] if src else []
        custom_cnt = _line_count(payload.get('custom_names_text'))
        pieces = [f"대상={len(src)}"]
        if str(payload.get('copy_count') or '').strip():
            pieces.append(f"복사수={payload.get('copy_count')}")
        if custom_cnt:
            pieces.append(f"지정명={custom_cnt}")
        tgt = str(payload.get('target_campaign_id') or '').strip()
        if tgt:
            pieces.append(f"타겟캠페인={tgt}")
        return " | ".join(pieces)
    if path == "/rename_adgroups_bulk":
        ids = payload.get('entity_ids') or []
        if not isinstance(ids, list):
            ids = [ids] if ids else []
        return f"그룹={len(ids)} | 새이름={_line_count(payload.get('target_names_text'))}"
    if path in {"/update_budget", "/set_campaign_state", "/update_media", "/update_adgroup_options", "/update_adgroup_search_options", "/update_schedule", "/update_schedule_campaign_bulk", "/update_non_search_keyword_exclusion", "/update_keyword_bids", "/update_bid_mode_by_scope", "/update_keyword_bids_by_search", "/update_keyword_bid_weights_by_search", "/adjust_keyword_bids_by_threshold", "/update_keyword_bids_avg_position", "/preview_keyword_avg_position_by_search", "/update_keyword_avg_position_by_search", "/update_powerlink_device_bid_weights"}:
        ids = payload.get('entity_ids') or payload.get('campaign_ids') or payload.get('adgroup_ids') or []
        if not isinstance(ids, list):
            ids = [ids] if ids else []
        label = str(payload.get('entity_type') or payload.get('media_type') or '').strip()
        extra = []
        if label:
            extra.append(label)
        if ids:
            extra.append(f"건수={len(ids)}")
        if str(payload.get('search_text') or '').strip():
            match_label = "완전일치" if _boolish(payload.get('exact_match'), False) else "부분일치"
            extra.append(f"검색={str(payload.get('search_text') or '').strip()} ({match_label})")
        if str(payload.get('bid_amt') or '').strip():
            extra.append(f"입찰가={payload.get('bid_amt')}")
        if str(payload.get('bid_weight') or payload.get('bidWeight') or '').strip():
            extra.append(f"입찰가중치={payload.get('bid_weight') or payload.get('bidWeight')}%")
        if str(payload.get('pc_bid_weight') or '').strip():
            extra.append(f"PC={payload.get('pc_bid_weight')}%")
        if str(payload.get('mobile_bid_weight') or '').strip():
            extra.append(f"MO={payload.get('mobile_bid_weight')}%")
        return " | ".join(extra)
    if path in {"/bulk_delete", "/delete_selected"}:
        rows = payload.get('rows') or payload.get('ids') or []
        if not isinstance(rows, list):
            rows = [rows] if rows else []
        return f"유형={str(payload.get('entity_type') or '').strip()} | 건수={len(rows)}"
    if path == "/bulk_delete_by_parent":
        ids = payload.get('parent_ids') or []
        if not isinstance(ids, list):
            ids = [ids] if ids else []
        return f"범위={str(payload.get('parent_type') or '').strip()} | 대상={str(payload.get('target_entity') or '').strip()} | 부모={len(ids)}"
    if path == "/bulk_register":
        rows = payload.get('rows') or []
        if not isinstance(rows, list):
            rows = [rows] if rows else []
        return f"유형={str(payload.get('entity_type') or '').strip()} | 건수={len(rows)}"
    if path == "/create_restricted_keywords_simple":
        return f"제외키워드={_line_count(payload.get('keywords_text'))}"
    if path in {"/bulk_upload_text_ads", "/bulk_upload_headlines"}:
        file_names = []
        try:
            file_names = [f.filename for f in request.files.values() if getattr(f, 'filename', '')]
        except Exception:
            file_names = []
        return f"파일={', '.join(file_names[:2])}" if file_names else "파일 업로드"
    count_keys = ('ids', 'entity_ids', 'source_ids', 'campaign_ids', 'adgroup_ids', 'parent_ids', 'rows')
    for key in count_keys:
        val = payload.get(key)
        if isinstance(val, list) and val:
            return f"건수={len(val)}"
    return ""
def _extract_response_message(response: Response) -> str:
    try:
        if response.is_json:
            data = response.get_json(silent=True) or {}
            if isinstance(data, dict):
                return str(data.get('message') or data.get('error') or data.get('details') or '').strip()
    except Exception:
        pass
    return ""
def _prune_action_log_file():
    try:
        if not os.path.exists(ACTION_LOG_PATH):
            return
        with open(ACTION_LOG_PATH, 'r', encoding='utf-8') as fp:
            lines = fp.readlines()
        if len(lines) <= _ACTION_LOG_MAX_LINES:
            return
        keep = lines[-_ACTION_LOG_MAX_LINES:]
        with open(ACTION_LOG_PATH, 'w', encoding='utf-8') as fp:
            fp.writelines(keep)
    except Exception:
        pass
def _append_action_log(entry: Dict[str, Any]):
    line = json.dumps(entry, ensure_ascii=False)
    with _ACTION_LOG_LOCK:
        with open(ACTION_LOG_PATH, 'a', encoding='utf-8') as fp:
            fp.write(line + "\n")
        _prune_action_log_file()
def _read_action_logs(limit: int = 150) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    with _ACTION_LOG_LOCK:
        if not os.path.exists(ACTION_LOG_PATH):
            return []
        with open(ACTION_LOG_PATH, 'r', encoding='utf-8') as fp:
            lines = fp.readlines()[-limit:]
    out: List[Dict[str, Any]] = []
    for line in reversed(lines):
        try:
            item = json.loads(line)
            if isinstance(item, dict):
                out.append(item)
        except Exception:
            continue
    return out
def _write_action_log_from_request(response: Response) -> None:
    path = request.path or ''
    if request.method != 'POST' or path in ACTION_LOG_EXCLUDED_PATHS:
        return
    try:
        payload = _safe_json_body()
        status = 'success' if 200 <= int(response.status_code) < 400 else 'error'
        customer_id = str(payload.get('customer_id') or payload.get('customerId') or '').strip()
        entry = {
            'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
            'path': path,
            'action': LOG_ACTION_LABELS.get(path, path.strip('/') or '요청'),
            'status': status,
            'http_status': int(response.status_code),
            'customer_id': customer_id,
            'summary': _action_summary(path, payload),
            'message': _extract_response_message(response),
        }
        _append_action_log(entry)
    except Exception:
        pass
DAY_NUM_TO_CODE = {1: "MON", 2: "TUE", 3: "WED", 4: "THU", 5: "FRI", 6: "SAT", 7: "SUN"}
SHOPPING_AD_TYPES = {"SHOPPING_PRODUCT_AD", "CATALOG_PRODUCT_AD", "CATALOG_AD", "SHOPPING_BRAND_AD"}
SHOPPING_TARGETABLE_ADGROUP_TYPES = {"SHOPPING", "CATALOG", "SHOPPING_BRAND"}
SHOPPING_ITEM_BID_AD_TYPES = {"SHOPPING_PRODUCT_AD", "CATALOG_PRODUCT_AD", "CATALOG_AD"}
SHOPPING_ADGROUP_TYPES_WITH_AD_LEVEL_BID = {"SHOPPING", "CATALOG"}
# Display labels are centralized in utils/labels.py.
# Keep operational type sets in app.py until the route/service split is complete.
COMMON_EXTENSION_TYPES = [
    "HEADLINE", "SUB_LINKS", "DESCRIPTION", "DESCRIPTION_EXTRA", "PHONE",
    "LOCATION", "PROMOTION", "PRICE_LINKS", "POWER_LINK_IMAGE", "WEBSITE_INFO", "IMAGE_SUB_LINKS"
]
SHOPPING_EXTRA_AD_TYPES = {"SHOPPING_PRODUCT_AD"}
SYSTEM_FIELDS = {
    "nccAdId", "adExtensionId", "nccKeywordId", "regTm", "editTm", "status", "statusReason",
    "inspectStatus", "delFlag", "managedKeyword", "referenceData", "referenceKeyData", "nccQi",
}
BOOL_FIELDS = {
    "useDailyBudget", "useGroupBidAmt", "userLock", "enable", "keywordPlusFlag", "useStoreUrl",
    "paused", "mobilePreferred", "descriptionPin", "headlinePin",
}
INT_FIELDS = {
    "customerId", "dailyBudget", "bidAmt", "contentsNetworkBidAmt", "priority", "displayOrder",
    "price", "discountPrice", "extraCost", "bidWeight",
}
FLOAT_FIELDS = {"pcCtr", "mobileCtr"}
ENTITY_SAMPLE_HEADERS: Dict[str, List[str]] = {
    "campaign": ["캠페인명", "캠페인유형", "일예산사용", "일예산"],
    "adgroup": ["캠페인ID", "광고그룹명", "광고그룹유형", "일예산사용", "일예산", "입찰가", "비즈채널ID"],
    "keyword": ["광고그룹ID", "키워드", "그룹입찰가사용", "입찰가", "사용자잠금"],
    "ad": ["광고그룹ID", "소재유형", "제목", "설명", "PC랜딩URL", "모바일랜딩URL", "사용자잠금"],
    "ad_extension": ["소유ID", "확장소재유형", "원본JSON"],
    "restricted_keyword": ["광고그룹ID", "제외키워드"],
}
DELETE_SAMPLE_HEADERS: Dict[str, List[str]] = {
    "campaign": ["캠페인ID"],
    "adgroup": ["광고그룹ID"],
    "keyword": ["키워드ID", "광고그룹ID", "키워드"],
    "ad": ["소재ID"],
    "ad_extension": ["확장소재ID"],
    "restricted_keyword": ["광고그룹ID", "제외키워드"],
}
KO_HEADER_ALIASES: Dict[str, str] = {
    "고객아이디": "customerId", "고객id": "customerId",
    "캠페인id": "nccCampaignId", "캠페인아이디": "nccCampaignId",
    "광고그룹id": "nccAdgroupId", "광고그룹아이디": "nccAdgroupId",
    "키워드id": "nccKeywordId", "키워드아이디": "nccKeywordId",
    "비즈채널id": "nccBusinessChannelId", "비즈채널아이디": "nccBusinessChannelId",
    "비즈채널": "nccBusinessChannelId",
    "소재id": "nccAdId", "소재아이디": "nccAdId",
    "확장소재id": "adExtensionId", "확장소재아이디": "adExtensionId",
    "소유id": "ownerId", "소유아이디": "ownerId",
    "캠페인명": "name", "캠페인유형": "campaignTp", "일예산사용": "useDailyBudget", "일예산": "dailyBudget",
    "광고그룹명": "name", "광고그룹유형": "adgroupType", "입찰가": "bidAmt",
    "키워드": "keyword", "제외키워드": "keyword", "그룹입찰가사용": "useGroupBidAmt", "사용자잠금": "userLock",
    "소재유형": "type", "제목": "headline", "설명": "description", "pc랜딩url": "pcFinalUrl",
    "모바일랜딩url": "mobileFinalUrl", "원본json": "rawJson",
    "확장소재유형": "type",
}
@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return jsonify({"error": e.description or str(e)}), int(getattr(e, "code", 500) or 500)
    return jsonify({"error": f"서버 내부 오류: {str(e)}"}), 500
@app.after_request
def after_request_write_action_log(response):
    _write_action_log_from_request(response)
    return response
@app.route("/favicon.ico")
def favicon():
    return Response(status=204)
def _display_url_from_final(final_url: str) -> str:
    s = str(final_url or "").strip()
    if not s:
        return s
    try:
        parsed = urlparse(s)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        pass
    return s
def _label_negative_type(value: Any) -> str:
    s = str(value or "").strip().upper()
    if s in {"2", "PHRASE", "구문"}:
        return "구문"
    if s in {"1", "EXACT", "일치"}:
        return "일치"
    if s in {"KEYWORD_PLUS_RESTRICT"}:
        return "일치"
    return s or "-"
def _normalize_negative_type(value: Any) -> int:
    s = str(value or "").strip().upper()
    if s in {"2", "PHRASE", "구문"}:
        return 2
    return 1
def _sig(ts: str, method: str, uri: str, secret_key: str) -> str:
    return make_signature(ts, method, uri, secret_key)
def _open_headers(api_key: str, secret_key: str, customer_id: str, method: str, uri: str) -> dict:
    return build_open_headers(api_key, secret_key, customer_id, method, uri)
def _make_fake_response(status_code: int, text: str):
    return make_fake_response(status_code, text)
def _do_req(method, api_key, secret_key, cid, uri, params=None, json_body=None, max_retries=3):
    return request_naver_api(
        method,
        api_key,
        secret_key,
        cid,
        uri,
        params=params,
        json_body=json_body,
        max_retries=max_retries,
        session=HTTP_SESSION,
        base_url=OPENAPI_BASE_URL,
    )
def _campaign_label(value: Any) -> str:
    return str(label_campaign_type(value, default=str(value or "-")))
def _adgroup_label(value: Any) -> str:
    return str(label_adgroup_type(value, default=str(value or "-")))
def _ad_label(value: Any) -> str:
    return str(label_ad_type(value, default=str(value or "-")))
def _extension_label(value: Any) -> str:
    return str(label_extension_type(value, default=str(value or "-")))
def _snake_to_camel(key: str) -> str:
    parts = str(key).strip().split("_")
    if len(parts) == 1:
        return parts[0]
    return parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:])
def _boolify(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "y", "yes", "t", "on"}
def _normalize_value(key: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
        lowered = value.lower()
        if lowered in {"예", "네", "사용", "y", "yes", "true", "on"}:
            value = True
        elif lowered in {"아니오", "미사용", "n", "no", "false", "off"}:
            value = False
        elif value.startswith("{") or value.startswith("["):
            try:
                return json.loads(value)
            except Exception:
                pass
    if key in BOOL_FIELDS:
        return _boolify(value)
    if key in INT_FIELDS:
        try:
            return int(float(str(value).replace(",", "").strip()))
        except Exception:
            return value
    if key in FLOAT_FIELDS:
        try:
            return float(str(value).replace(",", "").strip())
        except Exception:
            return value
    return value
def _strip_empty(data: Any) -> Any:
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            vv = _strip_empty(v)
            if vv is None or vv == "":
                continue
            cleaned[k] = vv
        return cleaned
    if isinstance(data, list):
        out = []
        for item in data:
            vv = _strip_empty(item)
            if vv is None:
                continue
            out.append(vv)
        return out
    return data
def _special_alias_map(entity_type: str) -> Dict[str, str]:
    base = {
        "customer_id": "customerId",
        "customerid": "customerId",
        "campaign_tp": "campaignTp",
        "daily_budget": "dailyBudget",
        "use_daily_budget": "useDailyBudget",
        "bid_amt": "bidAmt",
        "use_group_bid_amt": "useGroupBidAmt",
        "user_lock": "userLock",
        "owner_id": "ownerId",
        "reference_key": "referenceKey",
        "contents_network_bid_amt": "contentsNetworkBidAmt",
        "raw_json": "rawJson",
        "pc_final_url": "pcFinalUrl",
        "mobile_final_url": "mobileFinalUrl",
    }
    if entity_type == "adgroup":
        base.update({"campaign_id": "nccCampaignId"})
    elif entity_type in {"keyword", "ad", "restricted_keyword"}:
        base.update({"adgroup_id": "nccAdgroupId"})
    elif entity_type == "ad_extension":
        base.update({"adgroup_id": "ownerId"})
    return base
def _read_table_text(text: str) -> List[Dict[str, Any]]:
    if not text or not text.strip():
        return []
    sample = text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        sep = dialect.delimiter
    except Exception:
        sep = "\t" if "\t" in sample else ","
    df = pd.read_csv(io.StringIO(text), sep=sep, dtype=str, keep_default_na=False)
    renamed = []
    for c in df.columns:
        key = str(c).strip()
        key_norm = re.sub(r"\s+", "", key).lower()
        renamed.append(KO_HEADER_ALIASES.get(key_norm, key))
    df.columns = renamed
    return df.to_dict(orient="records")
def _result_item(row_no: int, ok: bool, name: str, detail: str = "") -> Dict[str, Any]:
    return {"row_no": row_no, "ok": ok, "name": name, "detail": detail}
def _read_uploaded_table(upload) -> List[Dict[str, Any]]:
    if upload is None or not getattr(upload, "filename", ""):
        return []
    filename = str(upload.filename or "").lower()
    raw = upload.read()
    try:
        upload.stream.seek(0)
    except Exception:
        pass
    if not raw:
        return []
    if filename.endswith('.csv') or filename.endswith('.txt') or filename.endswith('.tsv'):
        text = raw.decode('utf-8-sig', errors='ignore')
        return _read_table_text(text)
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        try:
            df = pd.read_excel(io.BytesIO(raw), dtype=str, keep_default_na=False)
        except Exception as e:
            raise ValueError(f'엑셀 파일을 읽지 못했습니다: {e}')
        renamed = []
        for c in df.columns:
            key = str(c).strip()
            key_norm = re.sub(r"\s+", "", key).lower()
            renamed.append(KO_HEADER_ALIASES.get(key_norm, key))
        df.columns = renamed
        return df.to_dict(orient='records')
    raise ValueError('지원 파일 형식은 csv, txt, tsv, xls, xlsx 입니다.')
def _looks_like_biz_channel_id(value: Any) -> bool:
    return bool(re.match(r"^bsn-", str(value or "").strip(), re.I))
def _normalize_campaign_tp(value: Any) -> str:
    s = str(value or "").strip().upper()
    if s in {"파워링크", "POWERLINK", "WEB", "WEB_SITE"}:
        return "WEB_SITE"
    if s in {"쇼핑검색", "쇼검", "SHOPPING"}:
        return "SHOPPING"
    if s in {"플레이스", "PLACE", "PLACE_AD"}:
        return "PLACE"
    if s in {"파워컨텐츠", "파워콘텐츠", "POWER_CONTENTS", "POWER_CONTENT", "POWERCONTENTS", "POWERCONTENT"}:
        return "POWER_CONTENTS"
    return s or "WEB_SITE"
def _is_shopping_campaign_type(value: Any) -> bool:
    s = str(value or "").strip().upper()
    if not s:
        return False
    return s in {"SHOPPING", "SHOPPING_BRAND", "CATALOG", "PRODUCT", "SHOPPING_PRODUCT"} or ("SHOPPING" in s) or ("CATALOG" in s) or ("PRODUCT" in s and s != "WEB_SITE")
def _default_adgroup_type_for_campaign(value: Any) -> str:
    s = str(value or "").strip().upper()
    if s == "SHOPPING_BRAND":
        return "SHOPPING_BRAND"
    if s == "CATALOG":
        return "CATALOG"
    if s == "SHOPPING":
        return "SHOPPING"
    if _is_shopping_campaign_type(s):
        return "SHOPPING"
    return "WEB_SITE"
def _normalize_adgroup_tp(value: Any, campaign_tp: str = "WEB_SITE") -> str:
    s = str(value or "").strip().upper()
    campaign_tp = str(campaign_tp or "").strip().upper()
    default_tp = _default_adgroup_type_for_campaign(campaign_tp)
    if s in {"", "기본", "파워링크", "WEB_SITE"}:
        return default_tp
    if s in {"쇼핑검색", "쇼검", "SHOPPING", "SHOPING", "PRODUCT", "SHOPPING_PRODUCT"}:
        return default_tp if _is_shopping_campaign_type(campaign_tp) else "WEB_SITE"
    if s in {"카탈로그", "CATALOG"}:
        return "CATALOG"
    if s in {"쇼핑브랜드", "SHOPPING_BRAND"}:
        return "SHOPPING_BRAND"
    return s
def _normalize_ad_type(value: Any) -> str:
    s = str(value or "").strip().upper()
    reverse = {v.upper(): k for k, v in AD_TYPE_LABELS.items()}
    if s in reverse:
        return reverse[s]
    if s in {"기본소재", "텍스트", "파워링크기본소재", "TEXT_45"}:
        return "TEXT_45"
    return s
def _normalize_extension_type(value: Any) -> str:
    s = str(value or "").strip().upper()
    reverse = {v.upper(): k for k, v in AD_EXTENSION_TYPE_LABELS.items()}
    if s in reverse:
        s = reverse[s]
    # API/화면/기존 데이터에서 섞여 들어오는 별칭을 최대한 같은 타입으로 정규화
    alias_map = {
        "SUB_LINK": "SUB_LINKS",
        "SUBLINK": "SUB_LINKS",
        "SUBLINKS": "SUB_LINKS",
        "HEAD_LINE": "HEADLINE",
        "HEADLINES": "HEADLINE",
        "HEAD LINE": "HEADLINE",
        "PROMO": "PROMOTION",
        "SHOPPING_PROMO_TEXT": "PROMOTION",
        "SHOPPING_EXTRA_INFO": "SHOPPING_EXTRA",
        "EXTRA_DESCRIPTION": "DESCRIPTION_EXTRA",
        "DESCRIPTIONEXTRA": "DESCRIPTION_EXTRA",
        "DESCRIPTION EXTRA": "DESCRIPTION_EXTRA",
    }
    if s in alias_map:
        return alias_map[s]
    if s == "쇼핑상품부가정보".upper():
        return "SHOPPING_EXTRA"
    return s
def _build_text45_ad_object(payload: Dict[str, Any], existing_ad: Any = None) -> Dict[str, Any]:
    ad_obj = copy.deepcopy(existing_ad) if isinstance(existing_ad, dict) else {}
    if isinstance(existing_ad, str):
        try:
            parsed = json.loads(existing_ad)
            if isinstance(parsed, dict):
                ad_obj = parsed
        except Exception:
            ad_obj = {}
    pc_obj = ad_obj.get("pc") if isinstance(ad_obj.get("pc"), dict) else {}
    mobile_obj = ad_obj.get("mobile") if isinstance(ad_obj.get("mobile"), dict) else {}
    fallback_final = str(payload.pop("finalUrl", "") or "").strip()
    headline = str(
        payload.pop("headline", payload.pop("title", ""))
        or ad_obj.get("headline")
        or ad_obj.get("title")
        or ""
    ).strip()
    description = str(
        payload.pop("description", "")
        or ad_obj.get("description")
        or ad_obj.get("desc")
        or ""
    ).strip()
    pc_final = str(payload.pop("pcFinalUrl", "") or pc_obj.get("final") or fallback_final).strip()
    mobile_final = str(
        payload.pop("mobileFinalUrl", payload.pop("mobileUrl", ""))
        or mobile_obj.get("final")
        or pc_final
        or fallback_final
    ).strip()
    merged = copy.deepcopy(ad_obj) if isinstance(ad_obj, dict) else {}
    if headline:
        merged["headline"] = headline
    if description:
        merged["description"] = description
    if pc_final:
        pc_payload = copy.deepcopy(pc_obj) if isinstance(pc_obj, dict) else {}
        pc_payload["final"] = pc_final
        merged["pc"] = pc_payload
    if mobile_final:
        mobile_payload = copy.deepcopy(mobile_obj) if isinstance(mobile_obj, dict) else {}
        mobile_payload["final"] = mobile_final
        merged["mobile"] = mobile_payload
    return _strip_empty(merged)
def _prepare_payload_row(row: Dict[str, Any], entity_type: str, cid: str) -> Dict[str, Any]:
    alias_map = _special_alias_map(entity_type)
    payload: Dict[str, Any] = {}
    for raw_key, raw_value in row.items():
        if raw_key is None:
            continue
        key0 = str(raw_key).strip()
        if not key0:
            continue
        key0_norm = re.sub(r"\s+", "", key0).lower()
        key = KO_HEADER_ALIASES.get(key0_norm, alias_map.get(key0, key0))
        if "_" in key and key not in {"nccCampaignId", "nccAdgroupId", "ownerId", "referenceKey", "rawJson"}:
            key = _snake_to_camel(key)
        value = _normalize_value(key, raw_value)
        if value is None:
            continue
        payload[key] = value
    payload["customerId"] = int(cid)
    if entity_type == "campaign":
        payload["campaignTp"] = _normalize_campaign_tp(payload.get("campaignTp"))
        payload.pop("nccCampaignId", None)
    if entity_type == "adgroup":
        campaign_tp = _normalize_campaign_tp(payload.get("campaignTp") or payload.get("parentCampaignTp") or "")
        payload["adgroupType"] = _normalize_adgroup_tp(payload.get("adgroupType"), campaign_tp)
        biz_channel_id = str(payload.get("nccBusinessChannelId") or "").strip()
        if biz_channel_id and _looks_like_biz_channel_id(biz_channel_id):
            payload.setdefault("pcChannelId", biz_channel_id)
            payload.setdefault("mobileChannelId", biz_channel_id)
        payload.pop("nccBusinessChannelId", None)
    if entity_type == "ad":
        ad_type = _normalize_ad_type(payload.get("type"))
        payload["type"] = ad_type
        raw_json = payload.pop("rawJson", None)
        if raw_json and isinstance(raw_json, str):
            try:
                raw_json = json.loads(raw_json)
            except Exception:
                raw_json = None
        if ad_type == "TEXT_45":
            existing_ad = payload.pop("ad", None)
            if isinstance(raw_json, dict):
                payload["ad"] = raw_json
            else:
                payload["ad"] = _build_text45_ad_object(payload, existing_ad)
        elif raw_json is not None:
            payload["ad"] = raw_json
        elif isinstance(payload.get("ad"), str):
            try:
                payload["ad"] = json.loads(payload["ad"])
            except Exception:
                pass
    if entity_type == "ad_extension":
        payload.pop("nccAdgroupId", None)
        payload["type"] = _normalize_extension_type(payload.get("type"))
        raw_json = payload.pop("rawJson", None)
        if raw_json and isinstance(raw_json, str):
            try:
                raw_payload = json.loads(raw_json)
                if isinstance(raw_payload, dict):
                    raw_payload.setdefault("ownerId", payload.get("ownerId"))
                    raw_payload.setdefault("type", payload.get("type"))
                    raw_payload.setdefault("customerId", int(cid))
                    payload = raw_payload
            except Exception:
                payload["rawJson"] = raw_json
    if entity_type == "restricted_keyword":
        rk_type = payload.get("type") or payload.get("matchType")
        payload = {
            "nccAdgroupId": str(payload.get("nccAdgroupId") or "").strip(),
            "customerId": int(cid),
            "keyword": str(payload.get("keyword") or payload.get("restrictedKeyword") or "").strip(),
        }
        if rk_type:
            payload["type"] = rk_type
    if entity_type != "ad":
        payload.pop("referenceData", None)
    for field in list(payload.keys()):
        if field in SYSTEM_FIELDS:
            payload.pop(field, None)
    return _strip_empty(payload)
def _normalize_campaign_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_campaign_item(item)
def _normalize_adgroup_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_adgroup_item(item)
def _normalize_channel_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_channel_item(item)
def _fetch_campaigns(api_key: str, secret_key: str, cid: str):
    return _LOOKUP_SERVICE.fetch_campaigns(api_key, secret_key, cid)
def _fetch_campaign_detail(api_key: str, secret_key: str, cid: str, campaign_id: str):
    return _LOOKUP_SERVICE.fetch_campaign_detail(api_key, secret_key, cid, campaign_id)
def _pc_mobile_label(pc: Any, mobile: Any) -> str:
    return pc_mobile_label(pc, mobile)
def _extract_pc_mobile_flags(detail_obj: Dict[str, Any] | None, pm_target_obj: Dict[str, Any] | None = None) -> Tuple[Optional[bool], Optional[bool]]:
    return extract_pc_mobile_flags(detail_obj, pm_target_obj)
def _enrich_adgroup_media_row(api_key: str, secret_key: str, cid: str, row: Dict[str, Any]) -> Dict[str, Any]:
    return _LOOKUP_SERVICE.enrich_adgroup_media_row(api_key, secret_key, cid, row, _fetch_target_object)
def _fetch_adgroups(api_key: str, secret_key: str, cid: str, campaign_id: str, enrich_media: bool = True):
    return _LOOKUP_SERVICE.fetch_adgroups(api_key, secret_key, cid, campaign_id, enrich_media=enrich_media, target_object_func=_fetch_target_object)
def _fetch_adgroup_detail(api_key: str, secret_key: str, cid: str, adgroup_id: str):
    return _LOOKUP_SERVICE.fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
def _fetch_first_biz_channel_id(api_key: str, secret_key: str, cid: str) -> str:
    return _LOOKUP_SERVICE.fetch_first_biz_channel_id(api_key, secret_key, cid)
def _is_shopping_adgroup(adgroup_obj: Dict[str, Any] | None) -> bool:
    adg_type = str((adgroup_obj or {}).get("adgroupType") or "").upper()
    return adg_type in {"SHOPPING", "CATALOG", "SHOPPING_BRAND"}
def _fetch_keywords(api_key: str, secret_key: str, cid: str, adgroup_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adgroup_id})
    if res.status_code != 200:
        return res, []
    return res, res.json() or []
def _normalize_keyword_search_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()
def _parse_keyword_search_groups(search_text: Any) -> List[str]:
    raw = str(search_text or "")
    parts = re.split(r"[\r\n,;|]+", raw)
    groups: List[str] = []
    seen: set[str] = set()
    for part in parts:
        norm = _normalize_keyword_search_text(part)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        groups.append(norm)
    if groups:
        return groups
    fallback = _normalize_keyword_search_text(raw)
    return [fallback] if fallback else []
def _keyword_match_terms(keyword: Any, search_text: Any, exact_match: bool = False, exclude_text: Any = None) -> List[str]:
    keyword_norm = _normalize_keyword_search_text(keyword)
    if not keyword_norm:
        return []
    if _keyword_matches_search(keyword_norm, exclude_text, exact_match=exact_match):
        return []
    groups = _parse_keyword_search_groups(search_text)
    matched: List[str] = []
    for group in groups:
        if not group:
            continue
        if bool(exact_match):
            if keyword_norm == group:
                matched.append(group)
            continue
        tokens = [tok for tok in group.split(" ") if tok]
        if tokens and all(tok in keyword_norm for tok in tokens):
            matched.append(group)
    return matched
def _keyword_matches_search(keyword: Any, search_text: Any, exact_match: bool = False) -> bool:
    keyword_norm = _normalize_keyword_search_text(keyword)
    if not keyword_norm:
        return False
    groups = _parse_keyword_search_groups(search_text)
    for group in groups:
        if not group:
            continue
        if bool(exact_match):
            if keyword_norm == group:
                return True
            continue
        tokens = [tok for tok in group.split(" ") if tok]
        if tokens and all(tok in keyword_norm for tok in tokens):
            return True
    return False
def _collect_powerlink_campaigns_and_adgroups(api_key: str, secret_key: str, cid: str, campaign_ids: List[str] | None = None):
    selected_ids = [str(x or "").strip() for x in (campaign_ids or []) if str(x or "").strip()]
    selected_set = set(selected_ids)
    res_camp, campaign_rows = _fetch_campaigns(api_key, secret_key, cid)
    if res_camp.status_code != 200:
        return res_camp, [], [], [f"캠페인 조회 실패: {res_camp.text}"]
    powerlink_campaigns = []
    skipped_non_powerlink: List[str] = []
    missing_selected = set(selected_ids)
    for row in (campaign_rows or []):
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        camp_id = str(row.get("id") or raw.get("nccCampaignId") or "").strip()
        camp_name = str(row.get("name") or raw.get("name") or camp_id).strip()
        camp_tp = str(row.get("campaignTp") or raw.get("campaignTp") or "").upper()
        if selected_set and camp_id not in selected_set:
            continue
        if camp_id in missing_selected:
            missing_selected.discard(camp_id)
        if camp_tp == "WEB_SITE":
            powerlink_campaigns.append({"id": camp_id, "name": camp_name})
        elif selected_set and camp_id:
            skipped_non_powerlink.append(f"{camp_name} ({camp_tp or '유형없음'})")
    warnings: List[str] = []
    if missing_selected:
        warnings.append(f"선택 캠페인 {len(missing_selected)}개를 찾지 못했습니다.")
    if skipped_non_powerlink:
        warnings.append(f"파워링크가 아닌 선택 캠페인 {len(skipped_non_powerlink)}개는 제외했습니다.")
    if not powerlink_campaigns:
        return res_camp, [], [], warnings[:10]
    adgroup_contexts: List[Dict[str, Any]] = []
    max_workers = min(max(FAST_IO_WORKERS, 8), max(1, len(powerlink_campaigns)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {
            ex.submit(_fetch_adgroups, api_key, secret_key, cid, camp["id"], False): camp
            for camp in powerlink_campaigns if camp.get("id")
        }
        for fut in as_completed(future_map):
            camp = future_map[fut]
            camp_id = str(camp.get("id") or "").strip()
            camp_name = str(camp.get("name") or camp_id)
            try:
                res_adg, rows = fut.result()
            except Exception as exc:
                warnings.append(f"캠페인 {camp_name} 광고그룹 조회 실패: {exc}")
                continue
            if res_adg.status_code != 200:
                warnings.append(f"캠페인 {camp_name} 광고그룹 조회 실패: {res_adg.text}")
                continue
            for row in (rows or []):
                raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
                adg_id = str(row.get("id") or raw.get("nccAdgroupId") or "").strip()
                adg_type = str(row.get("adgroupType") or raw.get("adgroupType") or "").upper()
                if not adg_id or adg_type != "WEB_SITE":
                    continue
                adgroup_contexts.append({
                    "campaign_id": camp_id,
                    "campaign_name": camp_name,
                    "adgroup_id": adg_id,
                    "adgroup_name": str(row.get("name") or raw.get("name") or adg_id),
                    "adgroup_bid": (raw or {}).get("bidAmt"),
                })
    adgroup_contexts.sort(key=lambda x: (str(x.get("campaign_name") or "").casefold(), str(x.get("adgroup_name") or "").casefold()))
    return res_camp, powerlink_campaigns, adgroup_contexts, warnings[:10]
def _scan_powerlink_keywords_by_search(api_key: str, secret_key: str, cid: str, search_text: str, exact_match: bool = False, campaign_ids: List[str] | None = None, adgroup_ids: List[str] | None = None, exclude_text: str = ""):
    _, powerlink_campaigns, adgroup_contexts, warnings = _collect_powerlink_campaigns_and_adgroups(api_key, secret_key, cid, campaign_ids=campaign_ids)
    selected_adgroup_ids = _unique_keep_order([str(x or "").strip() for x in (adgroup_ids or []) if str(x or "").strip()])
    if selected_adgroup_ids:
        selected_adgroup_set = set(selected_adgroup_ids)
        adgroup_contexts = [ctx for ctx in (adgroup_contexts or []) if str(ctx.get("adgroup_id") or "").strip() in selected_adgroup_set]
        selected_campaign_set = {str(ctx.get("campaign_id") or "").strip() for ctx in adgroup_contexts if str(ctx.get("campaign_id") or "").strip()}
        powerlink_campaigns = [row for row in (powerlink_campaigns or []) if str((row or {}).get("id") or "").strip() in selected_campaign_set]
    matched_rows: List[Dict[str, Any]] = []
    update_payload: List[Dict[str, Any]] = []
    updated_adgroup_ids: List[str] = []
    err_details: List[str] = list(warnings)
    cleanup_keys = ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']
    search_groups = _parse_keyword_search_groups(search_text)
    exclude_groups = _parse_keyword_search_groups(exclude_text)
    scanned_keyword_count = 0
    matched_count = 0
    if not powerlink_campaigns or not adgroup_contexts:
        return {
            "powerlink_campaigns": powerlink_campaigns,
            "adgroup_contexts": adgroup_contexts,
            "warnings": err_details[:10],
            "matched_rows": matched_rows,
            "update_payload": update_payload,
            "updated_adgroup_ids": updated_adgroup_ids,
            "scanned_keyword_count": 0,
            "matched_count": 0,
            "search_groups": search_groups,
            "exclude_groups": exclude_groups,
        }
    max_workers = min(max(BID_IO_WORKERS, 10), max(1, len(adgroup_contexts)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {
            ex.submit(_fetch_keywords, api_key, secret_key, cid, str(ctx.get("adgroup_id") or "")): ctx
            for ctx in adgroup_contexts if str(ctx.get("adgroup_id") or "").strip()
        }
        for fut in as_completed(future_map):
            ctx = future_map[fut]
            adg_id = str(ctx.get("adgroup_id") or "").strip()
            adg_name = str(ctx.get("adgroup_name") or adg_id)
            camp_name = str(ctx.get("campaign_name") or "")
            try:
                res_kw, kw_rows = fut.result()
            except Exception as exc:
                if len(err_details) < 10:
                    err_details.append(f"[{camp_name} > {adg_name}] 키워드 조회 실패: {exc}")
                continue
            if res_kw.status_code != 200:
                if len(err_details) < 10:
                    err_details.append(f"[{camp_name} > {adg_name}] 키워드 조회 실패: {res_kw.text}")
                continue
            adgroup_had_match = False
            for kw in (kw_rows or []):
                keyword_text = str((kw or {}).get("keyword") or "").strip()
                if not keyword_text:
                    continue
                scanned_keyword_count += 1
                matched_terms = _keyword_match_terms(keyword_text, search_text, exact_match=exact_match, exclude_text=exclude_text)
                if not matched_terms:
                    continue
                matched_count += 1
                current_bid = _normalize_bid_amt((kw or {}).get("bidAmt")) or 70
                current_use_group = bool((kw or {}).get("useGroupBidAmt"))
                try:
                    current_bid_weight = int((kw or {}).get("bidWeight") or 100)
                except Exception:
                    current_bid_weight = 100
                keyword_enabled = _extract_enabled_from_entity(kw)
                matched_rows.append({
                    "campaign_id": str(ctx.get("campaign_id") or "").strip(),
                    "campaign_name": camp_name,
                    "adgroup_id": adg_id,
                    "adgroup_name": adg_name,
                    "keyword": keyword_text,
                    "current_bid": int(current_bid),
                    "current_use_group": bool(current_use_group),
                    "current_bid_weight": int(current_bid_weight),
                    "current_state": "ON" if keyword_enabled is True else ("OFF" if keyword_enabled is False else "-"),
                    "enabled": keyword_enabled,
                    "status": str((kw or {}).get("status") or ""),
                    "inspect_status": str((kw or {}).get("inspectStatus") or ""),
                    "del_flag": bool((kw or {}).get("delFlag")),
                    "matched_terms": matched_terms,
                    "matched_terms_text": ", ".join(matched_terms),
                    "ncc_keyword_id": str((kw or {}).get("nccKeywordId") or "").strip(),
                })
                item = copy.deepcopy(kw)
                item["useGroupBidAmt"] = False
                for key in cleanup_keys:
                    item.pop(key, None)
                update_payload.append(item)
                adgroup_had_match = True
            if adgroup_had_match:
                updated_adgroup_ids.append(adg_id)
    matched_rows.sort(key=lambda x: (str(x.get("campaign_name") or "").casefold(), str(x.get("adgroup_name") or "").casefold(), str(x.get("keyword") or "").casefold()))
    return {
        "powerlink_campaigns": powerlink_campaigns,
        "adgroup_contexts": adgroup_contexts,
        "warnings": err_details[:10],
        "matched_rows": matched_rows,
        "update_payload": update_payload,
        "updated_adgroup_ids": updated_adgroup_ids,
        "scanned_keyword_count": int(scanned_keyword_count),
        "matched_count": int(matched_count),
        "search_groups": search_groups,
    }
def _extract_enabled_from_entity(entity: Dict[str, Any] | None) -> bool | None:
    src = entity or {}
    lock_val = src.get("userLock")
    if isinstance(lock_val, bool):
        return not lock_val
    enable_val = src.get("enable")
    if isinstance(enable_val, bool):
        return enable_val
    paused_val = src.get("paused")
    if isinstance(paused_val, bool):
        return not paused_val
    status = str(src.get("status") or "").upper()
    if status in {"PAUSE", "PAUSED", "STOP", "STOPPED", "LIMITEDBYBUDGET"}:
        return False
    if status:
        return True
    return None
def _set_keyword_state(api_key: str, secret_key: str, cid: str, keyword_id: str, enabled: bool) -> Tuple[bool, str]:
    keyword_id = str(keyword_id or "").strip()
    if not keyword_id:
        return False, "키워드 ID가 비어 있습니다."
    uri = f"/ncc/keywords/{keyword_id}"
    r_get = _do_req("GET", api_key, secret_key, cid, uri)
    if r_get.status_code != 200:
        return False, f"조회 실패: {r_get.text}"
    obj = r_get.json() or {}
    attempts: List[Tuple[str, Dict[str, Any]]] = []
    def _append_attempt(field_name: str, value: Any):
        body = copy.deepcopy(obj)
        body[field_name] = value
        attempts.append((field_name, body))
    if "userLock" in obj:
        _append_attempt("userLock", not enabled)
    if "enable" in obj:
        _append_attempt("enable", enabled)
    if "paused" in obj:
        _append_attempt("paused", not enabled)
    if not attempts:
        _append_attempt("userLock", not enabled)
        _append_attempt("enable", enabled)
    tried: set[str] = set()
    errors: List[str] = []
    for field_name, body in attempts:
        if field_name in tried:
            continue
        tried.add(field_name)
        r_put = _do_req("PUT", api_key, secret_key, cid, uri, params={"fields": field_name}, json_body=body)
        if r_put.status_code in [200, 201]:
            return True, ""
        errors.append(f"{field_name}: {r_put.text}")
    return False, " / ".join(errors) if errors else "상태 변경 실패"
def _build_powerlink_keyword_search_message(search_text: str, exact_match: bool, scan: Dict[str, Any], row_preview_limit: int = 50, exclude_text: str = "") -> str:
    powerlink_campaigns = scan.get("powerlink_campaigns") or []
    adgroup_contexts = scan.get("adgroup_contexts") or []
    matched_rows = scan.get("matched_rows") or []
    err_details = scan.get("warnings") or []
    search_groups = scan.get("search_groups") or _parse_keyword_search_groups(search_text)
    exclude_groups = scan.get("exclude_groups") or _parse_keyword_search_groups(exclude_text)
    scanned_keyword_count = int(scan.get("scanned_keyword_count") or 0)
    matched_count = int(scan.get("matched_count") or 0)
    mode_label = "완전일치" if exact_match else "부분일치"
    lines = [
        f"검색어 기준 파워링크 키워드 조회 완료! ({mode_label})",
        f"검색어: {search_text}",
        (f"제외어: {exclude_text}" if str(exclude_text or "").strip() else "제외어: 없음"),
        f"검색 그룹: {len(search_groups)}개",
        f"조회한 파워링크 캠페인: {len(powerlink_campaigns)}개 | 광고그룹: {len(adgroup_contexts)}개 | 키워드 스캔: {scanned_keyword_count}개",
        f"검색 일치 키워드: {matched_count}개",
    ]
    if matched_rows:
        lines.append("\n[일치 키워드 예시]")
        for row in matched_rows[:row_preview_limit]:
            current_label = f"{int(row.get('current_bid') or 0):,}원"
            if row.get("current_use_group"):
                current_label += " (그룹입찰가 사용)"
            matched_term_label = str(row.get("matched_terms_text") or "")
            if matched_term_label:
                matched_term_label = f" | 매칭: {matched_term_label}"
            lines.append(f"- {row.get('campaign_name')} > {row.get('adgroup_name')} > {row.get('keyword')}{matched_term_label} | 현재 {current_label}")
    if err_details:
        lines.append("\n[상세 내역]")
        lines.extend(err_details[:10])
    return "\n".join(lines)
def _build_powerlink_keyword_export_workbook(rows: List[Dict[str, Any]], search_text: str, exact_match: bool, exclude_text: str = ""):
    generated_at = time.strftime("%Y-%m-%d %H:%M:%S")
    mode_label = "완전일치" if exact_match else "부분일치"
    search_groups = _parse_keyword_search_groups(search_text)
    exclude_label = exclude_text if str(exclude_text or "").strip() else "없음"
    headers = ["번호", "캠페인명", "광고그룹명", "키워드", "매칭 검색어", "현재 입찰가(원)", "그룹입찰가 사용", "현재 입찰가중치(%)", "키워드 ID", "조회 방식"]
    data_rows = []
    for idx, row in enumerate(rows, start=1):
        data_rows.append([
            idx,
            str(row.get("campaign_name") or ""),
            str(row.get("adgroup_name") or ""),
            str(row.get("keyword") or ""),
            str(row.get("matched_terms_text") or ""),
            int(row.get("current_bid") or 0),
            "Y" if row.get("current_use_group") else "N",
            int(row.get("current_bid_weight") or 100),
            str(row.get("ncc_keyword_id") or ""),
            mode_label,
        ])
    widths = {1: 8, 2: 24, 3: 24, 4: 32, 5: 24, 6: 16, 7: 14, 8: 18, 9: 22, 10: 14}
    return build_report_workbook(
        title="파워링크 키워드 조회 결과",
        sheet_title="키워드조회결과",
        metadata=[
            f"생성시각: {generated_at}",
            f"조회조건: {mode_label} / 검색어 {len(search_groups)}개 / {search_text}",
            f"제외어: {exclude_label}",
        ],
        headers=headers,
        rows=data_rows,
        start_row=6,
        widths=widths,
        freeze_panes="A7",
    )
def _find_powerlink_duplicate_keywords(api_key: str, secret_key: str, cid: str, campaign_ids: List[str], duplicate_scope: str = "selected_campaigns", adgroup_ids: List[str] | None = None, target_scope: str = "selected_campaigns"):
    selected_ids = _unique_keep_order([str(x or "").strip() for x in (campaign_ids or []) if str(x or "").strip()])
    selected_adgroup_ids = _unique_keep_order([str(x or "").strip() for x in (adgroup_ids or []) if str(x or "").strip()])
    target_scope = str(target_scope or "selected_campaigns").strip().lower()
    if target_scope not in {"selected_campaigns", "selected_adgroups"}:
        target_scope = "selected_adgroups" if selected_adgroup_ids else "selected_campaigns"
    scope = str(duplicate_scope or "selected_campaigns").strip().lower()
    if scope not in {"selected_campaigns", "within_campaign", "campaign_internal"}:
        scope = "selected_campaigns"

    if target_scope == "selected_campaigns" and not selected_ids:
        return {"rows": [], "message": "선택한 캠페인이 없습니다.", "skipped": [], "errors": [], "duplicate_scope": scope, "target_scope": target_scope}
    if target_scope == "selected_adgroups" and not selected_adgroup_ids:
        return {"rows": [], "message": "선택한 광고그룹이 없습니다.", "skipped": [], "errors": [], "duplicate_scope": scope, "target_scope": target_scope, "checked_adgroup_count": 0, "powerlink_adgroup_count": 0}

    errors: List[str] = []
    skipped: List[str] = []
    powerlink_targets: List[Tuple[str, str]] = []
    adgroup_contexts: List[Dict[str, str]] = []

    if target_scope == "selected_adgroups":
        # 선택 그룹 기준은 체크한 광고그룹만 키워드 스캔 대상으로 제한한다.
        # campaign_ids가 넘어오면 해당 캠페인 안에서 먼저 찾고, 누락 시 전체 파워링크 캠페인에서 보정 탐색한다.
        res_camp, powerlink_campaigns, all_contexts, warnings = _collect_powerlink_campaigns_and_adgroups(
            api_key, secret_key, cid, campaign_ids=selected_ids or None
        )
        if res_camp.status_code != 200:
            raise RuntimeError(f"캠페인 조회 실패: {res_camp.text}")
        skipped.extend(warnings or [])
        selected_adgroup_set = set(selected_adgroup_ids)
        adgroup_contexts = [ctx for ctx in (all_contexts or []) if str(ctx.get("adgroup_id") or "").strip() in selected_adgroup_set]
        found_adgroup_ids = {str(ctx.get("adgroup_id") or "").strip() for ctx in adgroup_contexts if str(ctx.get("adgroup_id") or "").strip()}

        if len(found_adgroup_ids) < len(selected_adgroup_set) and selected_ids:
            res_camp2, powerlink_campaigns2, all_contexts2, warnings2 = _collect_powerlink_campaigns_and_adgroups(
                api_key, secret_key, cid, campaign_ids=None
            )
            if res_camp2.status_code == 200:
                existing = {str(ctx.get("adgroup_id") or "").strip() for ctx in adgroup_contexts}
                for ctx in (all_contexts2 or []):
                    adg_id = str(ctx.get("adgroup_id") or "").strip()
                    if adg_id in selected_adgroup_set and adg_id not in existing:
                        adgroup_contexts.append(ctx)
                        existing.add(adg_id)
                merged_campaign_ids = {str(c.get("id") or "").strip() for c in (powerlink_campaigns or []) if str(c.get("id") or "").strip()}
                powerlink_campaigns = list(powerlink_campaigns or [])
                for c in (powerlink_campaigns2 or []):
                    cidv = str(c.get("id") or "").strip()
                    if cidv and cidv not in merged_campaign_ids:
                        powerlink_campaigns.append(c)
                        merged_campaign_ids.add(cidv)
                skipped.extend(warnings2 or [])
            else:
                skipped.append(f"선택 광고그룹 보정 조회 실패: {res_camp2.text}")
            found_adgroup_ids = {str(ctx.get("adgroup_id") or "").strip() for ctx in adgroup_contexts if str(ctx.get("adgroup_id") or "").strip()}

        missing_adgroup_ids = [x for x in selected_adgroup_ids if x not in found_adgroup_ids]
        if missing_adgroup_ids:
            skipped.append(f"선택 광고그룹 {len(missing_adgroup_ids)}개를 찾지 못했거나 파워링크 그룹이 아닙니다.")
        selected_campaign_set = {str(ctx.get("campaign_id") or "").strip() for ctx in adgroup_contexts if str(ctx.get("campaign_id") or "").strip()}
        for camp in (powerlink_campaigns or []):
            camp_id = str(camp.get("id") or "").strip()
            if camp_id and camp_id in selected_campaign_set:
                powerlink_targets.append((camp_id, str(camp.get("name") or camp_id)))
    else:
        res_camp, campaign_rows = _fetch_campaigns(api_key, secret_key, cid)
        if res_camp.status_code != 200:
            raise RuntimeError(f"캠페인 조회 실패: {res_camp.text}")
        campaign_map = {}
        for row in campaign_rows or []:
            camp_id = str(row.get("id") or row.get("nccCampaignId") or "").strip()
            if camp_id:
                campaign_map[camp_id] = row
        for camp_id in selected_ids:
            row = campaign_map.get(camp_id)
            if not row:
                skipped.append(f"{camp_id} (캠페인 조회 실패 또는 없음)")
                continue
            camp_name = str(row.get("name") or camp_id)
            camp_tp = str(row.get("campaignTp") or "").upper()
            if camp_tp != "WEB_SITE":
                skipped.append(f"{camp_name} ({camp_tp or '유형없음'})")
                continue
            powerlink_targets.append((camp_id, camp_name))

        for camp_id, camp_name in powerlink_targets:
            res_adg, adgroup_rows = _fetch_adgroups(api_key, secret_key, cid, camp_id, enrich_media=False)
            if res_adg.status_code != 200:
                errors.append(f"[{camp_name}] 광고그룹 조회 실패: {res_adg.text}")
                continue
            for row in adgroup_rows or []:
                raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
                adgroup_id = str(row.get("id") or row.get("nccAdgroupId") or raw.get("nccAdgroupId") or "").strip()
                if not adgroup_id:
                    continue
                adgroup_contexts.append({
                    "campaign_id": camp_id,
                    "campaign_name": camp_name,
                    "adgroup_id": adgroup_id,
                    "adgroup_name": str(row.get("name") or raw.get("name") or adgroup_id),
                })

    all_keyword_map: Dict[str, Dict[str, Any]] = {}
    max_workers = max(1, min(FAST_IO_WORKERS, len(adgroup_contexts) or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {
            ex.submit(_fetch_keywords, api_key, secret_key, cid, str(ctx.get("adgroup_id") or "")): ctx
            for ctx in adgroup_contexts if str(ctx.get("adgroup_id") or "").strip()
        }
        for fut in as_completed(future_map):
            ctx = future_map[fut]
            camp_id = str(ctx.get("campaign_id") or "").strip()
            camp_name = str(ctx.get("campaign_name") or camp_id)
            adg_id = str(ctx.get("adgroup_id") or "").strip()
            adg_name = str(ctx.get("adgroup_name") or adg_id)
            try:
                res_kw, kw_rows = fut.result()
            except Exception as e:
                errors.append(f"[{camp_name} > {adg_name}] 키워드 조회 실패: {e}")
                continue
            if res_kw.status_code != 200:
                errors.append(f"[{camp_name} > {adg_name}] 키워드 조회 실패: {res_kw.text}")
                continue
            seen_in_group = set()
            for kw in kw_rows or []:
                keyword = str(kw.get("keyword") or "").strip()
                if not keyword:
                    continue
                norm = re.sub(r"\s+", " ", keyword).strip().casefold()
                if not norm or norm in seen_in_group:
                    continue
                seen_in_group.add(norm)
                entry = all_keyword_map.setdefault(norm, {"keyword": keyword, "occurrences": []})
                entry["occurrences"].append({
                    "campaign_id": camp_id,
                    "campaign_name": camp_name,
                    "adgroup_id": adg_id,
                    "adgroup_name": adg_name,
                    "keyword_id": str(kw.get("nccKeywordId") or kw.get("id") or "").strip(),
                })

    rows_out: List[Dict[str, Any]] = []
    target_scope_label = "선택 광고그룹" if target_scope == "selected_adgroups" else "선택 캠페인"
    scope_label = f"{target_scope_label} 전체"

    def _append_duplicate_row(keyword: str, occurrences: List[Dict[str, Any]]):
        uniq_locations = []
        seen_location_keys = set()
        for occ in occurrences:
            key = f"{occ.get('campaign_id') or ''}|{occ.get('adgroup_id') or ''}"
            if not key.strip('|') or key in seen_location_keys:
                continue
            seen_location_keys.add(key)
            uniq_locations.append(occ)
        if len(uniq_locations) < 2:
            return
        campaign_names = []
        campaign_ids_out = []
        adgroups = []
        for occ in uniq_locations:
            cname = str(occ.get("campaign_name") or occ.get("campaign_id") or "")
            cidv = str(occ.get("campaign_id") or "")
            if cname and cname not in campaign_names:
                campaign_names.append(cname)
            if cidv and cidv not in campaign_ids_out:
                campaign_ids_out.append(cidv)
            adgroups.append({
                "id": str(occ.get("adgroup_id") or ""),
                "name": str(occ.get("adgroup_name") or occ.get("adgroup_id") or ""),
                "campaign_id": cidv,
                "campaign_name": cname,
            })
        adgroup_names = [f"[{g.get('campaign_name')}] {g.get('name')}" if len(campaign_names) > 1 else str(g.get("name") or "") for g in adgroups]
        campaign_name_text = ", ".join(campaign_names[:3]) + (f" 외 {len(campaign_names) - 3}개" if len(campaign_names) > 3 else "")
        rows_out.append({
            "campaign_id": ", ".join(campaign_ids_out),
            "campaign_name": campaign_name_text,
            "campaign_names": campaign_names,
            "campaign_count": len(campaign_names),
            "keyword": keyword,
            "adgroup_count": len(adgroups),
            "adgroups": adgroups,
            "adgroup_names": adgroup_names,
            "adgroup_names_text": ", ".join(adgroup_names),
            "duplicate_scope": scope,
            "duplicate_scope_label": scope_label,
            "target_scope": target_scope,
            "target_scope_label": target_scope_label,
        })

    if scope in {"within_campaign", "campaign_internal"}:
        per_campaign: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for norm, item in all_keyword_map.items():
            keyword = item.get("keyword") or ""
            for occ in item.get("occurrences") or []:
                key = (str(occ.get("campaign_id") or ""), norm)
                entry = per_campaign.setdefault(key, {"keyword": keyword, "occurrences": []})
                entry["occurrences"].append(occ)
        scope_label = f"{target_scope_label} / 캠페인 내부"
        for item in per_campaign.values():
            _append_duplicate_row(str(item.get("keyword") or ""), list(item.get("occurrences") or []))
    else:
        scope_label = f"{target_scope_label} 전체"
        for item in all_keyword_map.values():
            _append_duplicate_row(str(item.get("keyword") or ""), list(item.get("occurrences") or []))

    # scope_label은 중복 판단 후 확정되므로 행별 표기에도 최신 라벨을 맞춘다.
    for row in rows_out:
        row["duplicate_scope_label"] = scope_label
        row["target_scope_label"] = target_scope_label

    rows_out.sort(key=lambda x: (-int(x.get("campaign_count") or 0), str(x.get("keyword") or "").casefold(), str(x.get("campaign_name") or "").casefold()))
    msg_parts = []
    if target_scope == "selected_adgroups":
        msg_parts.append(f"파워링크 광고그룹 {len(adgroup_contexts)}개 기준")
    elif powerlink_targets:
        msg_parts.append(f"파워링크 캠페인 {len(powerlink_targets)}개 기준")
    msg_parts.append(scope_label)
    if rows_out:
        msg_parts.append(f"중복 키워드 {len(rows_out)}개 발견")
    else:
        msg_parts.append("중복 키워드가 없습니다.")
    if skipped:
        msg_parts.append(f"제외/안내 {len(skipped)}개")
    return {
        "rows": rows_out,
        "message": " / ".join(msg_parts),
        "skipped": skipped,
        "errors": errors[:30],
        "checked_campaign_count": len(selected_ids),
        "powerlink_campaign_count": len(powerlink_targets),
        "checked_adgroup_count": len(selected_adgroup_ids),
        "powerlink_adgroup_count": len(adgroup_contexts),
        "duplicate_scope": scope,
        "duplicate_scope_label": scope_label,
        "target_scope": target_scope,
        "target_scope_label": target_scope_label,
    }

def _build_powerlink_duplicate_keyword_workbook(result: Dict[str, Any], scope_label: str = "선택 캠페인"):
    generated_at = time.strftime("%Y-%m-%d %H:%M:%S")
    rows = list((result or {}).get("rows") or [])
    skipped = list((result or {}).get("skipped") or [])
    errors = list((result or {}).get("errors") or [])
    message = str((result or {}).get("message") or "")
    headers = ["번호", "캠페인명", "중복 캠페인 수", "키워드", "중복 광고그룹 수", "광고그룹명", "캠페인 ID", "광고그룹 ID 목록", "비고"]
    data_rows = []
    for idx, row in enumerate(rows, start=1):
        adgroups = list(row.get("adgroups") or [])
        adgroup_names = row.get("adgroup_names") or [str(g.get("name") or g.get("id") or "") for g in adgroups]
        adgroup_ids = [str(g.get("id") or "") for g in adgroups if str(g.get("id") or "").strip()]
        data_rows.append([
            idx,
            str(row.get("campaign_name") or ""),
            int(row.get("campaign_count") or len(row.get("campaign_names") or []) or 1),
            str(row.get("keyword") or ""),
            int(row.get("adgroup_count") or len(adgroup_names) or 0),
            ", ".join(str(x) for x in adgroup_names if str(x).strip()),
            str(row.get("campaign_id") or ""),
            ", ".join(adgroup_ids),
            str(row.get("duplicate_scope_label") or row.get("duplicate_scope") or ""),
        ])
    widths = {1: 8, 2: 36, 3: 16, 4: 32, 5: 16, 6: 68, 7: 36, 8: 68, 9: 22}
    return build_report_workbook(
        title="파워링크 중복키워드 조회 결과",
        sheet_title="중복키워드",
        metadata=[
            f"생성시각: {generated_at}",
            f"조회범위: {scope_label}",
            f"요약: {message}",
            f"제외 캠페인: {', '.join(skipped) if skipped else '없음'}",
            f"오류: {' / '.join(errors) if errors else '없음'}",
        ],
        headers=headers,
        rows=data_rows,
        start_row=8,
        widths=widths,
        freeze_panes="A9",
    )
def _find_account_powerlink_duplicate_keywords(api_key: str, secret_key: str, cid: str):
    res_camp, campaign_rows = _fetch_campaigns(api_key, secret_key, cid)
    if res_camp.status_code != 200:
        raise RuntimeError(f"캠페인 조회 실패: {res_camp.text}")
    powerlink_ids: List[str] = []
    for row in campaign_rows or []:
        camp_id = str(row.get("id") or row.get("nccCampaignId") or "").strip()
        camp_tp = str(row.get("campaignTp") or "").upper()
        if camp_id and camp_tp == "WEB_SITE":
            powerlink_ids.append(camp_id)
    if not powerlink_ids:
        return {"rows": [], "message": "현재 계정에 파워링크 캠페인이 없습니다.", "skipped": [], "errors": [], "checked_campaign_count": 0, "powerlink_campaign_count": 0}
    result = _find_powerlink_duplicate_keywords(api_key, secret_key, cid, powerlink_ids, duplicate_scope="selected_campaigns")
    rows = list(result.get("rows") or [])
    rows.sort(key=lambda x: (-int(x.get("adgroup_count") or 0), str(x.get("keyword") or "").casefold(), str(x.get("campaign_name") or "").casefold()))
    result["rows"] = rows
    found = len(rows)
    result["message"] = f"현재 계정 파워링크 캠페인 {len(powerlink_ids)}개 기준 / 중복 키워드 {found}개 발견" if found else f"현재 계정 파워링크 캠페인 {len(powerlink_ids)}개 기준 / 중복 키워드가 없습니다."
    result["checked_campaign_count"] = len(powerlink_ids)
    result["powerlink_campaign_count"] = len(powerlink_ids)
    return result
def _get_powerlink_keyword_stats(api_key: str, secret_key: str, cid: str, campaign_ids: List[str] | None = None):
    selected_ids = [str(x or "").strip() for x in (campaign_ids or []) if str(x or "").strip()]
    res_camp, campaign_rows = _fetch_campaigns(api_key, secret_key, cid)
    if res_camp.status_code != 200:
        raise RuntimeError(f"캠페인 조회 실패: {res_camp.text}")
    campaign_map: Dict[str, Dict[str, Any]] = {}
    powerlink_targets: List[Tuple[str, str]] = []
    for row in campaign_rows or []:
        camp_id = str(row.get("id") or row.get("nccCampaignId") or "").strip()
        if not camp_id:
            continue
        campaign_map[camp_id] = row
        if str(row.get("campaignTp") or "").upper() == "WEB_SITE":
            powerlink_targets.append((camp_id, str(row.get("name") or camp_id)))
    skipped: List[str] = []
    selected_powerlink_targets: List[Tuple[str, str]] = []
    for camp_id in selected_ids:
        row = campaign_map.get(camp_id)
        if not row:
            skipped.append(f"{camp_id} (캠페인 조회 실패 또는 없음)")
            continue
        camp_name = str(row.get("name") or camp_id)
        camp_tp = str(row.get("campaignTp") or "").upper()
        if camp_tp != "WEB_SITE":
            skipped.append(f"{camp_name} ({camp_tp or '유형없음'})")
            continue
        selected_powerlink_targets.append((camp_id, camp_name))
    campaign_adgroups: Dict[str, List[Dict[str, Any]]] = {}
    errors: List[str] = []
    if powerlink_targets:
        max_workers = max(1, min(FAST_IO_WORKERS, len(powerlink_targets)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {
                ex.submit(_fetch_adgroups, api_key, secret_key, cid, camp_id, False): (camp_id, camp_name)
                for camp_id, camp_name in powerlink_targets
            }
            for fut in as_completed(future_map):
                camp_id, camp_name = future_map[fut]
                try:
                    res_adg, adgroup_rows = fut.result()
                except Exception as e:
                    errors.append(f"[{camp_name}] 광고그룹 조회 실패: {e}")
                    continue
                if res_adg.status_code != 200:
                    errors.append(f"[{camp_name}] 광고그룹 조회 실패: {res_adg.text}")
                    continue
                campaign_adgroups[camp_id] = adgroup_rows or []
    keyword_tasks: List[Tuple[str, str, str, str]] = []
    total_adgroup_count = 0
    for camp_id, camp_name in powerlink_targets:
        adgroup_rows = campaign_adgroups.get(camp_id) or []
        for row in adgroup_rows:
            adg_id = str(row.get("id") or row.get("nccAdgroupId") or "").strip()
            if not adg_id:
                continue
            adg_name = str(row.get("name") or adg_id)
            keyword_tasks.append((camp_id, camp_name, adg_id, adg_name))
            total_adgroup_count += 1
    campaign_keyword_count: Dict[str, int] = defaultdict(int)
    campaign_adgroup_count: Dict[str, int] = defaultdict(int)
    for camp_id, _, _, _ in keyword_tasks:
        campaign_adgroup_count[camp_id] += 1
    total_keyword_count = 0
    if keyword_tasks:
        max_workers = max(1, min(max(FAST_IO_WORKERS, 12), len(keyword_tasks)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {
                ex.submit(_fetch_keywords, api_key, secret_key, cid, adg_id): (camp_id, camp_name, adg_id, adg_name)
                for camp_id, camp_name, adg_id, adg_name in keyword_tasks
            }
            for fut in as_completed(future_map):
                camp_id, camp_name, adg_id, adg_name = future_map[fut]
                try:
                    res_kw, kw_rows = fut.result()
                except Exception as e:
                    errors.append(f"[{camp_name} > {adg_name}] 키워드 조회 실패: {e}")
                    continue
                if res_kw.status_code != 200:
                    errors.append(f"[{camp_name} > {adg_name}] 키워드 조회 실패: {res_kw.text}")
                    continue
                kw_cnt = sum(1 for kw in (kw_rows or []) if str((kw or {}).get("keyword") or "").strip())
                campaign_keyword_count[camp_id] += kw_cnt
                total_keyword_count += kw_cnt
    campaign_stats = []
    for camp_id, camp_name in selected_powerlink_targets:
        campaign_stats.append({
            "campaign_id": camp_id,
            "campaign_name": camp_name,
            "keyword_count": int(campaign_keyword_count.get(camp_id) or 0),
            "adgroup_count": int(campaign_adgroup_count.get(camp_id) or 0),
        })
    campaign_stats.sort(key=lambda x: (-int(x.get("keyword_count") or 0), str(x.get("campaign_name") or "").casefold()))
    msg_parts = [
        f"현재 계정 파워링크 캠페인 {len(powerlink_targets)}개",
        f"총 등록 키워드 {total_keyword_count}개",
    ]
    if selected_ids:
        if selected_powerlink_targets:
            msg_parts.append(f"체크한 파워링크 캠페인 {len(selected_powerlink_targets)}개")
        else:
            msg_parts.append("체크한 파워링크 캠페인이 없습니다.")
    if skipped:
        msg_parts.append(f"제외 {len(skipped)}개")
    return {
        "message": " / ".join(msg_parts),
        "total_keyword_count": int(total_keyword_count),
        "total_powerlink_campaign_count": len(powerlink_targets),
        "total_powerlink_adgroup_count": int(total_adgroup_count),
        "checked_campaign_count": len(selected_ids),
        "selected_powerlink_campaign_count": len(selected_powerlink_targets),
        "campaign_stats": campaign_stats,
        "skipped": skipped,
        "errors": errors[:50],
    }
def _fetch_ads(api_key: str, secret_key: str, cid: str, adgroup_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": adgroup_id})
    if res.status_code != 200:
        return res, []
    return res, res.json() or []
def _resolve_adgroup_contexts(api_key: str, secret_key: str, cid: str, entity_type: str, entity_ids: List[str]):
    contexts: List[Dict[str, Any]] = []
    warnings: List[str] = []
    seen: set[str] = set()
    entity_type = str(entity_type or "adgroup").strip().lower()
    entity_ids = _unique_keep_order(entity_ids or [])
    def _append_context(adgroup_id: str, obj: Dict[str, Any] | None, fallback_name: str = ""):
        adgroup_id = str(adgroup_id or "").strip()
        if not adgroup_id or adgroup_id in seen:
            return
        obj = obj or {}
        raw = obj.get("raw") if isinstance(obj, dict) and isinstance(obj.get("raw"), dict) else obj
        contexts.append({
            "adgroup_id": adgroup_id,
            "adgroup_type": str((obj or {}).get("adgroupType") or (raw or {}).get("adgroupType") or "").upper(),
            "adgroup_bid": (raw or {}).get("bidAmt"),
            "name": str((obj or {}).get("name") or (raw or {}).get("name") or fallback_name or ""),
        })
        seen.add(adgroup_id)
    if entity_type == "campaign":
        for camp_id in entity_ids:
            camp_id = str(camp_id or "").strip()
            if not camp_id:
                continue
            res, rows = _fetch_adgroups(api_key, secret_key, cid, camp_id)
            if res.status_code != 200:
                warnings.append(f"캠페인 {camp_id} 하위 광고그룹 조회 실패: {res.text}")
                continue
            for row in rows:
                _append_context(str(row.get("id") or row.get("nccAdgroupId") or ""), row, fallback_name=str(row.get("name") or ""))
    else:
        for adg_id in entity_ids:
            adg_id = str(adg_id or "").strip()
            if not adg_id:
                continue
            res, obj = _fetch_adgroup_detail(api_key, secret_key, cid, adg_id)
            if res.status_code != 200 or not obj:
                warnings.append(f"광고그룹 {adg_id} 조회 실패: {res.text}")
                continue
            _append_context(str(obj.get("nccAdgroupId") or adg_id), obj, fallback_name=str(obj.get("name") or adg_id))
    return contexts, warnings

def _split_filter_words(value: Any) -> List[str]:
    raw = value
    if isinstance(raw, (list, tuple, set)):
        parts = []
        for item in raw:
            parts.extend(str(item or "").replace("，", ",").replace(";", ",").replace("\n", ",").split(","))
    else:
        parts = str(raw or "").replace("，", ",").replace(";", ",").replace("\n", ",").split(",")
    return [x.strip().lower() for x in parts if x and x.strip()]

def _adgroup_name_word_match(name: str, word: str, match_mode: str = "contains") -> bool:
    name_norm = str(name or "").strip().lower()
    word_norm = str(word or "").strip().lower()
    if not word_norm:
        return False
    if str(match_mode or "").strip().lower() in {"exact", "equals", "equal", "정확히일치", "완전일치"}:
        return name_norm == word_norm
    return word_norm in name_norm

def _filter_adgroup_contexts_by_name_conditions(adgroup_contexts: List[Dict[str, Any]], payload: Dict[str, Any]):
    include_words = _split_filter_words(payload.get("adgroup_name_include") or payload.get("adgroup_include_words") or payload.get("include_adgroup_words"))
    exclude_words = _split_filter_words(payload.get("adgroup_name_exclude") or payload.get("adgroup_exclude_words") or payload.get("exclude_adgroup_words"))
    match_mode = str(payload.get("adgroup_name_match_mode") or payload.get("adgroup_match_mode") or "contains").strip().lower()
    if not include_words and not exclude_words:
        return adgroup_contexts, {"include_words": [], "exclude_words": [], "match_mode": match_mode, "included_skipped": 0, "excluded_skipped": 0}
    kept: List[Dict[str, Any]] = []
    included_skipped = 0
    excluded_skipped = 0
    for ctx in adgroup_contexts or []:
        name = str((ctx or {}).get("name") or "")
        if include_words and not any(_adgroup_name_word_match(name, w, match_mode) for w in include_words):
            included_skipped += 1
            continue
        if exclude_words and any(_adgroup_name_word_match(name, w, match_mode) for w in exclude_words):
            excluded_skipped += 1
            continue
        kept.append(ctx)
    return kept, {"include_words": include_words, "exclude_words": exclude_words, "match_mode": match_mode, "included_skipped": included_skipped, "excluded_skipped": excluded_skipped}

def _adgroup_name_filter_summary(filter_info: Dict[str, Any]) -> List[str]:
    if not filter_info:
        return []
    lines: List[str] = []
    include_words = filter_info.get("include_words") or []
    exclude_words = filter_info.get("exclude_words") or []
    if include_words or exclude_words:
        bits = []
        if include_words:
            bits.append("포함 단어: " + ", ".join(include_words))
        if exclude_words:
            bits.append("제외 단어: " + ", ".join(exclude_words))
        lines.append("광고그룹명 조건 필터 적용 - " + " / ".join(bits))
    included_skipped = int(filter_info.get("included_skipped") or 0)
    excluded_skipped = int(filter_info.get("excluded_skipped") or 0)
    if included_skipped or excluded_skipped:
        lines.append(f"광고그룹명 조건으로 제외된 광고그룹: 포함조건 미충족 {included_skipped}개 / 제외단어 매칭 {excluded_skipped}개")
    return lines

def _adgroup_uses_ad_level_bid(adgroup_type: str) -> bool:
    return str(adgroup_type or "").upper() in SHOPPING_ADGROUP_TYPES_WITH_AD_LEVEL_BID

def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None

def _bool_or_none(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"", "none", "null", "undefined"}:
        return None
    if s in {"1", "true", "t", "y", "yes", "on", "사용", "예", "네"}:
        return True
    if s in {"0", "false", "f", "n", "no", "off", "미사용", "아니오"}:
        return False
    return None

def _parse_json_obj_if_needed(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return copy.deepcopy(value)
    if isinstance(value, str) and value.strip().startswith("{"):
        try:
            parsed = json.loads(value)
            return copy.deepcopy(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

def _extract_ad_attr(ad_item: Dict[str, Any] | None) -> Dict[str, Any]:
    """Return ad-level bid attrs across the response shapes used by Naver Ads.

    Ads may expose bid settings as top-level adAttr, nested ad.adAttr, or as an
    adAttrJson string. The old export checked only top-level adAttr, so bid
    columns could be blank even when the API response had the values.
    """
    item = ad_item or {}
    candidates: List[Dict[str, Any]] = []
    for key in ("adAttr", "ad_attr", "adAttrs", "adAttrJson", "ad_attr_json"):
        obj = _parse_json_obj_if_needed(item.get(key))
        if obj:
            candidates.append(obj)
    ad_obj = item.get("ad") if isinstance(item.get("ad"), dict) else {}
    for key in ("adAttr", "ad_attr", "adAttrs", "adAttrJson", "ad_attr_json"):
        obj = _parse_json_obj_if_needed(ad_obj.get(key))
        if obj:
            candidates.append(obj)
    merged: Dict[str, Any] = {}
    for obj in candidates:
        merged.update(obj)
    direct_bid = _first_non_empty(item.get("bidAmt"), item.get("bid_amt"), item.get("adBidAmt"), item.get("ad_bid_amt"))
    direct_use_group = _first_non_empty(item.get("useGroupBidAmt"), item.get("use_group_bid_amt"), item.get("adUseGroupBidAmt"), item.get("ad_use_group_bid_amt"))
    if direct_bid is not None and merged.get("bidAmt") is None:
        merged["bidAmt"] = direct_bid
    if direct_use_group is not None and merged.get("useGroupBidAmt") is None:
        merged["useGroupBidAmt"] = direct_use_group
    return merged

def _ad_item_has_bid_attr(ad_item: Dict[str, Any] | None) -> bool:
    if str((ad_item or {}).get("type") or "").upper() in SHOPPING_ITEM_BID_AD_TYPES:
        return True
    ad_attr = _extract_ad_attr(ad_item)
    return any(k in ad_attr for k in ("bidAmt", "useGroupBidAmt", "adBidAmt", "adUseGroupBidAmt"))

def _resolve_effective_bid(raw_bid: Any, use_group_bid: bool, adgroup_bid: Any) -> Optional[int]:
    if bool(use_group_bid):
        group_bid = _normalize_bid_amt(adgroup_bid)
        if group_bid is not None:
            return group_bid
    return _normalize_bid_amt(raw_bid)
def _put_single_ad_with_ad_attr(api_key: str, secret_key: str, cid: str, ad_item: Dict[str, Any]):
    ad_id = _extract_ad_id(ad_item)
    cleanup_keys = ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'referenceKey', 'referenceData', 'referenceKeyData']
    item = copy.deepcopy(ad_item)
    for k in cleanup_keys:
        item.pop(k, None)
    res = _do_req("PUT", api_key, secret_key, cid, f"/ncc/ads/{ad_id}", params={"fields": "adAttr"}, json_body=item)
    if res.status_code in [200, 201]:
        return res
    fallback = _do_req("PUT", api_key, secret_key, cid, "/ncc/ads", params={"fields": "adAttr"}, json_body=[item])
    if fallback.status_code in [200, 201]:
        return fallback
    return res
def _apply_ad_bid_map(api_key: str, secret_key: str, cid: str, adgroup_contexts: List[Dict[str, Any]], bid_map: Dict[str, int], ad_meta: Optional[Dict[str, Dict[str, Any]]] = None):
    success_cnt = fail_cnt = skipped_cnt = 0
    err_details: List[str] = []
    ad_meta = ad_meta or {}
    for ctx in adgroup_contexts:
        if not _adgroup_uses_ad_level_bid(ctx.get("adgroup_type")):
            continue
        adg_id = str(ctx.get("adgroup_id") or "").strip()
        res_ads, ads = _fetch_ads(api_key, secret_key, cid, adg_id)
        if res_ads.status_code != 200:
            fail_cnt += 1
            if len(err_details) < 5:
                err_details.append(f"[광고그룹 {adg_id}] 소재 조회 실패: {res_ads.text}")
            continue
        for ad in (ads or []):
            if not _ad_item_has_bid_attr(ad):
                continue
            ad_id = _extract_ad_id(ad)
            if not ad_id:
                continue
            if ad_id not in bid_map:
                skipped_cnt += 1
                continue
            meta = ad_meta.get(ad_id, {})
            current_use_group = bool(meta.get("use_group_bid", _extract_ad_attr(ad).get("useGroupBidAmt")))
            current_bid = _normalize_bid_amt(meta.get("current_bid"))
            target_bid = int(bid_map[ad_id])
            if current_bid == target_bid and (not current_use_group):
                skipped_cnt += 1
                continue
            item = copy.deepcopy(ad)
            ad_attr = _extract_ad_attr(item)
            ad_attr["useGroupBidAmt"] = False
            ad_attr["bidAmt"] = target_bid
            item["adAttr"] = ad_attr
            r_put = _put_single_ad_with_ad_attr(api_key, secret_key, cid, item)
            if r_put.status_code in [200, 201]:
                success_cnt += 1
            else:
                fail_cnt += 1
                if len(err_details) < 5:
                    name = str(meta.get("name") or ((ad.get("ad") or {}).get("productName") if isinstance(ad.get("ad"), dict) else "") or ad_id)
                    err_details.append(f"[{name}] 변경 실패: {r_put.text}")
    return success_cnt, fail_cnt, skipped_cnt, err_details
def _resolve_shopping_extra_owner_ids(api_key: str, secret_key: str, cid: str, campaign_ids: List[str] | None = None, adgroup_ids: List[str] | None = None):
    campaign_ids = [str(x).strip() for x in (campaign_ids or []) if str(x).strip()]
    adgroup_ids = [str(x).strip() for x in (adgroup_ids or []) if str(x).strip()]
    candidate_adgroup_ids: List[str] = []
    seen_adgroups: set[str] = set()
    warnings: List[str] = []
    for adgroup_id in adgroup_ids:
        if adgroup_id and adgroup_id not in seen_adgroups:
            candidate_adgroup_ids.append(adgroup_id)
            seen_adgroups.add(adgroup_id)
    for campaign_id in campaign_ids:
        res_adg, rows = _fetch_adgroups(api_key, secret_key, cid, campaign_id)
        if res_adg.status_code != 200:
            warnings.append(f"캠페인 {campaign_id} 광고그룹 조회 실패: {res_adg.text}")
            continue
        for row in rows:
            raw = row.get("raw") if isinstance(row, dict) else {}
            adg_type = str((raw or {}).get("adgroupType") or row.get("adgroupType") or "").upper()
            if adg_type != "SHOPPING":
                continue
            adg_id = str(row.get("id") or row.get("nccAdgroupId") or "").strip()
            if adg_id and adg_id not in seen_adgroups:
                candidate_adgroup_ids.append(adg_id)
                seen_adgroups.add(adg_id)
    owner_ids: List[str] = []
    seen_owner_ids: set[str] = set()
    for adgroup_id in candidate_adgroup_ids:
        res_ads, ads = _fetch_ads(api_key, secret_key, cid, adgroup_id)
        if res_ads.status_code != 200:
            warnings.append(f"광고그룹 {adgroup_id} 소재 조회 실패: {res_ads.text}")
            continue
        for ad in (ads or []):
            if not isinstance(ad, dict):
                continue
            ad_type = str(ad.get("type") or ad.get("adType") or "").upper()
            if ad_type not in SHOPPING_EXTRA_AD_TYPES:
                continue
            owner_id = str(ad.get("nccAdId") or ad.get("id") or "").strip()
            if owner_id and owner_id not in seen_owner_ids:
                owner_ids.append(owner_id)
                seen_owner_ids.add(owner_id)
    return owner_ids, warnings
def _fetch_extensions(api_key: str, secret_key: str, cid: str, owner_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": owner_id})
    if res.status_code != 200:
        return res, []
    return res, res.json() or []

def _extract_ad_extension_id(ext_item: Dict[str, Any] | None) -> str:
    """Return the real Naver ad-extension id from all known response shapes.

    Some API responses use `adExtensionId`; others use `nccAdExtensionId`.
    Export/delete/copy should not leave the extension ID blank because of alias differences.
    """
    if not isinstance(ext_item, dict):
        return ""
    candidates = [
        ext_item.get("adExtensionId"),
        ext_item.get("nccAdExtensionId"),
        ext_item.get("nccAdExtId"),
        ext_item.get("adExtId"),
        ext_item.get("id"),
    ]
    nested = ext_item.get("adExtension")
    if isinstance(nested, dict):
        candidates.extend([
            nested.get("adExtensionId"),
            nested.get("nccAdExtensionId"),
            nested.get("nccAdExtId"),
            nested.get("id"),
        ])
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return ""
def _extract_extension_image_ids(ext_item: Dict[str, Any] | None) -> str:
    """Extract image identifiers from image-type ad-extension responses.

    IMAGE_SUB_LINKS can return image identifiers in different nested shapes
    depending on the response/detail shape. Scan image-related id keys inside
    adExtension/sub-link items so the export does not leave the image ID blank.
    """
    if not isinstance(ext_item, dict):
        return ""
    raw_type = str(ext_item.get("type") or ext_item.get("adExtensionType") or "").strip()
    normalized_type = _normalize_extension_type(raw_type)
    if normalized_type not in {"IMAGE_SUB_LINKS", "POWER_LINK_IMAGE"} and "IMAGE" not in raw_type.upper():
        return ""

    exact_compact_keys = {
        "imageid", "nccimageid", "imageassetid", "imagefileid", "imageresourceid",
        "creativeimageid", "pcimageid", "mobileimageid", "imgid", "imgno",
        "imageno", "imageuid", "fileid", "assetid", "id",
    }
    generic_compact_keys = {"fileid", "assetid", "id"}
    found: List[str] = []
    seen: set[str] = set()

    def add_value(value: Any):
        if isinstance(value, bool) or value is None:
            return
        if isinstance(value, (dict, list, tuple)):
            walk(value, "")
            return
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "undefined"}:
            return
        if text.startswith("http://") or text.startswith("https://"):
            return
        if len(text) > 160:
            return
        if text not in seen:
            seen.add(text)
            found.append(text)

    def is_image_id_key(key: Any, path: str = "") -> bool:
        k_raw = str(key or "").strip()
        k = k_raw.lower()
        compact = re.sub(r"[^a-z0-9]", "", k)
        path_l = str(path or "").lower()
        if compact in exact_compact_keys:
            if compact in generic_compact_keys:
                return "image" in path_l or "img" in path_l
            return True
        return (("image" in k or "img" in k) and ("id" in k or compact.endswith("no")))

    def maybe_parse_json_string(value: str) -> Any:
        text = str(value or "").strip()
        if not text or text[0] not in "[{":
            return None
        try:
            return json.loads(text)
        except Exception:
            return None

    def walk(obj: Any, path: str = ""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                next_path = f"{path}.{key}" if path else str(key)
                if is_image_id_key(key, path):
                    add_value(value)
                else:
                    walk(value, next_path)
        elif isinstance(obj, (list, tuple)):
            for idx, item in enumerate(obj):
                walk(item, f"{path}[{idx}]")
        elif isinstance(obj, str):
            parsed = maybe_parse_json_string(obj)
            if parsed is not None:
                walk(parsed, path)

    walk(ext_item)
    return ", ".join(found)

def _normalize_lookup_scope(value: Any) -> str:
    scope = str(value or "account").strip().lower()
    if scope in {"selected_campaigns", "campaigns", "campaign_only"}:
        return "campaign"
    return "campaign" if scope == "campaign" else "account"
def _collect_asset_scope_adgroups(api_key: str, secret_key: str, cid: str, scope: str, campaign_ids: List[str] | None = None):
    scope = _normalize_lookup_scope(scope)
    selected_campaign_ids = _unique_keep_order([str(x or "").strip() for x in (campaign_ids or []) if str(x or "").strip()])
    res_camp, all_campaigns = _fetch_campaigns(api_key, secret_key, cid)
    if res_camp.status_code != 200:
        return res_camp, [], [], []
    campaign_map = {str((row or {}).get("id") or "").strip(): row for row in (all_campaigns or []) if str((row or {}).get("id") or "").strip()}
    if scope == "campaign":
        if not selected_campaign_ids:
            return _make_fake_response(400, "선택 캠페인 조회를 사용하려면 좌측에서 캠페인을 체크해주세요."), [], [], []
        target_campaigns = []
        missing_campaign_ids: List[str] = []
        for campaign_id in selected_campaign_ids:
            row = campaign_map.get(campaign_id)
            if row:
                target_campaigns.append(row)
            else:
                missing_campaign_ids.append(campaign_id)
        if missing_campaign_ids:
            for campaign_id in missing_campaign_ids:
                target_campaigns.append({"id": campaign_id, "name": campaign_id, "campaignTp": "", "label": "캠페인"})
    else:
        target_campaigns = list(all_campaigns or [])
        missing_campaign_ids = []
    contexts: List[Dict[str, Any]] = []
    warnings: List[str] = []
    if not target_campaigns:
        return _make_fake_response(200, "대상 캠페인 없음"), [], warnings, target_campaigns
    max_workers = min(FAST_IO_WORKERS, max(1, len(target_campaigns)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(_fetch_adgroups, api_key, secret_key, cid, str(camp.get("id") or ""), False): camp for camp in target_campaigns}
        for fut in as_completed(future_map):
            camp = future_map[fut]
            camp_id = str((camp or {}).get("id") or "").strip()
            camp_name = str((camp or {}).get("name") or camp_id)
            camp_type = str((camp or {}).get("campaignTp") or "")
            try:
                res_adg, adg_rows = fut.result()
            except Exception as e:
                warnings.append(f"캠페인 {camp_name} 광고그룹 조회 실패: {e}")
                continue
            if res_adg.status_code != 200:
                warnings.append(f"캠페인 {camp_name} 광고그룹 조회 실패: {res_adg.text}")
                continue
            for adg in (adg_rows or []):
                adg_id = str((adg or {}).get("id") or (adg or {}).get("nccAdgroupId") or "").strip()
                if not adg_id:
                    continue
                contexts.append({
                    "campaign_id": camp_id,
                    "campaign_name": camp_name,
                    "campaign_type": camp_type,
                    "adgroup_id": adg_id,
                    "adgroup_name": str((adg or {}).get("name") or adg_id),
                    "adgroup_type": str((adg or {}).get("adgroupType") or ""),
                    "adgroup_bid": ((adg or {}).get("raw") if isinstance((adg or {}).get("raw"), dict) else (adg or {})).get("bidAmt"),
                })
    if not contexts and warnings:
        return _make_fake_response(400, "\n".join(warnings[:10])), [], warnings, target_campaigns
    return _make_fake_response(200, "OK"), contexts, warnings, target_campaigns
def _summarize_lookup_ad(ad_item: Dict[str, Any] | None) -> str:
    ad_item = ad_item or {}
    ad_obj = ad_item.get("ad") if isinstance(ad_item.get("ad"), dict) else {}
    pieces = [
        str(ad_obj.get("headline") or "").strip(),
        str(ad_obj.get("productName") or "").strip(),
        str(ad_obj.get("name") or "").strip(),
        str(ad_obj.get("title") or "").strip(),
        str(ad_item.get("name") or "").strip(),
        str(ad_item.get("type") or "").strip(),
    ]
    for value in pieces:
        if value:
            return value
    return json.dumps(ad_item, ensure_ascii=False)[:120]
def _lookup_bid_display_value(value: Any) -> Any:
    try:
        bid = int(float(value))
    except Exception:
        return ""
    return bid if bid > 0 else ""

def _lookup_ad_bid_fields(ad_item: Dict[str, Any] | None, adgroup_bid: Any = None) -> Dict[str, Any]:
    ad_item = ad_item or {}
    ad_attr = _extract_ad_attr(ad_item)
    raw_ad_bid = _first_non_empty(ad_attr.get("bidAmt"), ad_attr.get("adBidAmt"), ad_attr.get("bid_amt"), ad_attr.get("ad_bid_amt"))
    use_group_raw = _first_non_empty(ad_attr.get("useGroupBidAmt"), ad_attr.get("adUseGroupBidAmt"), ad_attr.get("use_group_bid_amt"), ad_attr.get("ad_use_group_bid_amt"))
    parsed_use_group = _bool_or_none(use_group_raw)
    group_bid_display = _lookup_bid_display_value(adgroup_bid)
    has_raw_ad_bid = _normalize_bid_amt(raw_ad_bid) is not None
    # If the ad response has no ad-level bid fields, it is effectively using the
    # adgroup bid. Show that explicitly instead of leaving every bid column blank.
    use_group = parsed_use_group if parsed_use_group is not None else (not has_raw_ad_bid)
    effective_bid = _resolve_effective_bid(raw_ad_bid, use_group, adgroup_bid)
    if effective_bid is None and group_bid_display:
        effective_bid = _normalize_bid_amt(adgroup_bid)
    return {
        "adBidAmt": _lookup_bid_display_value(raw_ad_bid),
        "adUseGroupBidAmt": "Y" if use_group else "N",
        "adgroupBidAmt": group_bid_display,
        "effectiveBidAmt": _lookup_bid_display_value(effective_bid),
    }

def _summarize_lookup_extension(ext_item: Dict[str, Any] | None) -> str:
    ext_item = ext_item or {}
    ad_ext = ext_item.get("adExtension") if isinstance(ext_item.get("adExtension"), dict) else {}
    candidates = [
        str(ad_ext.get("headline") or "").strip(),
        str(ad_ext.get("description") or "").strip(),
        str(ad_ext.get("additionalDescription") or "").strip(),
        str(ad_ext.get("promotionTitle") or "").strip(),
        str(ad_ext.get("sellerName") or "").strip(),
        str(ad_ext.get("price") or "").strip(),
        str(ext_item.get("type") or "").strip(),
    ]
    for value in candidates:
        if value:
            return value
    links = ad_ext.get("subLinks") or ad_ext.get("subLinksExtension") or []
    if isinstance(links, list) and links:
        titles = []
        for item in links[:3]:
            if isinstance(item, dict):
                title = str(item.get("title") or item.get("name") or item.get("headline") or "").strip()
                if title:
                    titles.append(title)
        if titles:
            return " / ".join(titles)
    return json.dumps(ext_item, ensure_ascii=False)[:120]
def _collect_lookup_ads_for_contexts(api_key: str, secret_key: str, cid: str, contexts: List[Dict[str, Any]]):
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    if not contexts:
        return rows, warnings
    max_workers = min(FAST_IO_WORKERS, max(1, len(contexts)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(_fetch_ads, api_key, secret_key, cid, str(ctx.get("adgroup_id") or "")): ctx for ctx in contexts}
        for fut in as_completed(future_map):
            ctx = future_map[fut]
            adgroup_id = str(ctx.get("adgroup_id") or "")
            try:
                res_ads, ads = fut.result()
            except Exception as e:
                warnings.append(f"광고그룹 {adgroup_id} 소재 조회 실패: {e}")
                continue
            if res_ads.status_code != 200:
                warnings.append(f"광고그룹 {adgroup_id} 소재 조회 실패: {res_ads.text}")
                continue
            for ad in (ads or []):
                ad = ad if isinstance(ad, dict) else {}
                ad_id = str(ad.get("nccAdId") or ad.get("id") or "").strip()
                bid_fields = _lookup_ad_bid_fields(ad, ctx.get("adgroup_bid"))
                rows.append({
                    "campaignId": str(ctx.get("campaign_id") or ""),
                    "campaignName": str(ctx.get("campaign_name") or ""),
                    "campaignType": str(ctx.get("campaign_type") or ""),
                    "adgroupId": adgroup_id,
                    "adgroupName": str(ctx.get("adgroup_name") or ""),
                    "adgroupType": str(ctx.get("adgroup_type") or ""),
                    "adId": ad_id,
                    "type": str(ad.get("type") or ad.get("adType") or ""),
                    "status": "ON" if _extract_enabled_from_entity(ad) is not False else "OFF",
                    "summary": _summarize_lookup_ad(ad),
                    **bid_fields,
                })
    rows.sort(key=lambda x: (x.get("campaignName") or "", x.get("adgroupName") or "", x.get("type") or "", x.get("summary") or ""))
    return rows, warnings

def _collect_lookup_keywords_for_contexts(api_key: str, secret_key: str, cid: str, contexts: List[Dict[str, Any]]):
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    if not contexts:
        return rows, warnings
    max_workers = min(FAST_IO_WORKERS, max(1, len(contexts)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(_fetch_keywords, api_key, secret_key, cid, str(ctx.get("adgroup_id") or "")): ctx for ctx in contexts}
        for fut in as_completed(future_map):
            ctx = future_map[fut]
            adgroup_id = str(ctx.get("adgroup_id") or "")
            try:
                res_kw, kw_rows = fut.result()
            except Exception as e:
                warnings.append(f"광고그룹 {adgroup_id} 키워드 조회 실패: {e}")
                continue
            if res_kw.status_code != 200:
                warnings.append(f"광고그룹 {adgroup_id} 키워드 조회 실패: {res_kw.text}")
                continue
            for kw in (kw_rows or []):
                kw = kw if isinstance(kw, dict) else {}
                rows.append({
                    "campaignId": str(ctx.get("campaign_id") or ""),
                    "campaignName": str(ctx.get("campaign_name") or ""),
                    "campaignType": str(ctx.get("campaign_type") or ""),
                    "adgroupId": adgroup_id,
                    "adgroupName": str(ctx.get("adgroup_name") or ""),
                    "adgroupType": str(ctx.get("adgroup_type") or ""),
                    "keywordId": str(kw.get("nccKeywordId") or kw.get("id") or "").strip(),
                    "keyword": str(kw.get("keyword") or "").strip(),
                    "status": "ON" if _extract_enabled_from_entity(kw) is not False else "OFF",
                    "bidAmt": kw.get("bidAmt"),
                    "useGroupBidAmt": bool(kw.get("useGroupBidAmt")),
                    "matchType": str(kw.get("matchType") or kw.get("type") or "").strip(),
                })
    rows.sort(key=lambda x: (x.get("campaignName") or "", x.get("adgroupName") or "", x.get("keyword") or ""))
    return rows, warnings
def _collect_lookup_extensions_for_contexts(api_key: str, secret_key: str, cid: str, contexts: List[Dict[str, Any]], ad_rows: List[Dict[str, Any]] | None = None):
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    owner_jobs: List[Tuple[str, Dict[str, Any], str]] = []
    seen_jobs: set[Tuple[str, str]] = set()
    for ctx in contexts or []:
        owner_id = str(ctx.get("adgroup_id") or "").strip()
        if owner_id and (owner_id, "ADGROUP") not in seen_jobs:
            owner_jobs.append((owner_id, ctx, "ADGROUP"))
            seen_jobs.add((owner_id, "ADGROUP"))
    for ad_row in (ad_rows or []):
        owner_id = str(ad_row.get("adId") or "").strip()
        if not owner_id or (owner_id, "AD") in seen_jobs:
            continue
        ctx = {
            "campaign_id": ad_row.get("campaignId"),
            "campaign_name": ad_row.get("campaignName"),
            "campaign_type": ad_row.get("campaignType"),
            "adgroup_id": ad_row.get("adgroupId"),
            "adgroup_name": ad_row.get("adgroupName"),
            "adgroup_type": ad_row.get("adgroupType"),
        }
        owner_jobs.append((owner_id, ctx, "AD"))
        seen_jobs.add((owner_id, "AD"))
    if not owner_jobs:
        return rows, warnings
    max_workers = min(FAST_IO_WORKERS, max(1, len(owner_jobs)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(_fetch_extensions, api_key, secret_key, cid, owner_id): (owner_id, ctx, owner_scope) for owner_id, ctx, owner_scope in owner_jobs}
        for fut in as_completed(future_map):
            owner_id, ctx, owner_scope = future_map[fut]
            try:
                res_ext, ext_rows = fut.result()
            except Exception as e:
                warnings.append(f"owner {owner_id} 확장소재 조회 실패: {e}")
                continue
            if res_ext.status_code != 200:
                warnings.append(f"owner {owner_id} 확장소재 조회 실패: {res_ext.text}")
                continue
            for ext in (ext_rows or []):
                ext = ext if isinstance(ext, dict) else {}
                rows.append({
                    "campaignId": str(ctx.get("campaign_id") or ""),
                    "campaignName": str(ctx.get("campaign_name") or ""),
                    "campaignType": str(ctx.get("campaign_type") or ""),
                    "adgroupId": str(ctx.get("adgroup_id") or ""),
                    "adgroupName": str(ctx.get("adgroup_name") or ""),
                    "adgroupType": str(ctx.get("adgroup_type") or ""),
                    "ownerId": owner_id,
                    "ownerScope": "소재" if owner_scope == "AD" else "광고그룹",
                    "adExtensionId": _extract_ad_extension_id(ext),
                    "imageId": _extract_extension_image_ids(ext),
                    "type": str(ext.get("type") or ext.get("adExtensionType") or ""),
                    "status": "ON" if _extract_enabled_from_entity(ext) is not False else "OFF",
                    "summary": _summarize_lookup_extension(ext),
                })
    rows.sort(key=lambda x: (x.get("campaignName") or "", x.get("adgroupName") or "", x.get("ownerScope") or "", x.get("type") or "", x.get("summary") or ""))
    return rows, warnings
def _build_asset_lookup_workbook(rows: List[Dict[str, Any]], title: str, scope_label: str, columns: List[Tuple[str, str]]):
    def _format_lookup_excel_value(key: str, label: str, value: Any) -> Any:
        return format_asset_lookup_excel_value(
            key,
            label,
            value,
            normalize_ad_type=_normalize_ad_type,
            normalize_extension_type=_normalize_extension_type,
        )

    generated_at = time.strftime("%Y-%m-%d %H:%M:%S")
    headers = [label for _, label in columns]
    data_rows = []
    for row in rows:
        data_rows.append([
            _format_lookup_excel_value(key, label, row.get(key))
            for key, label in columns
        ])
    width_by_key = {
        "campaignType": 14, "campaignName": 22, "adgroupType": 16, "adgroupName": 22, "summary": 36, "type": 18, "status": 10,
        "effectiveBidAmt": 14, "adBidAmt": 14, "adUseGroupBidAmt": 16, "adgroupBidAmt": 16, "adId": 24, "adExtensionId": 24, "imageId": 28, "ownerId": 24, "campaignId": 20, "adgroupId": 22, "ownerScope": 12,
    }
    widths = {
        idx: width_by_key.get(key, max(12, min(40, len(label) + 4)))
        for idx, (key, label) in enumerate(columns, start=1)
    }
    return build_report_workbook(
        title=title,
        sheet_title=title[:31],
        metadata=[
            f"생성시각: {generated_at}",
            f"조회범위: {scope_label}",
        ],
        headers=headers,
        rows=data_rows,
        start_row=5,
        widths=widths,
        freeze_panes="A6",
    )
def _fetch_target_restrict_object(api_key: str, secret_key: str, cid: str, owner_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/targets", params={"ownerId": owner_id, "types": "RESTRICT_KEYWORD_TARGET"})
    if res.status_code != 200:
        return res, None
    data = res.json() or []
    if isinstance(data, dict):
        data = [data]
    for item in data:
        if isinstance(item, dict) and item.get("targetTp") == "RESTRICT_KEYWORD_TARGET":
            return res, item
    return res, None
def _fetch_target_object(api_key: str, secret_key: str, cid: str, owner_id: str, target_type: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/targets", params={"ownerId": owner_id, "types": target_type})
    if res.status_code != 200:
        return res, None
    data = res.json() or []
    if isinstance(data, dict):
        data = [data]
    for item in data:
        if isinstance(item, dict) and str(item.get("targetTp") or "").upper() == str(target_type or "").upper():
            return res, item
    return res, None
def _fetch_non_search_keyword_target(api_key: str, secret_key: str, cid: str, owner_id: str):
    return _fetch_target_object(api_key, secret_key, cid, owner_id, "NON_SEARCH_KEYWORD_TARGET")
def _get_pc_mobile_tuple(media_type: Any) -> Tuple[bool, bool]:
    media = str(media_type or "ALL").strip().upper()
    if media == "PC":
        return True, False
    if media == "MOBILE":
        return False, True
    return True, True
def _normalize_media_detail(media_detail: Any) -> Dict[str, bool]:
    raw = media_detail if isinstance(media_detail, dict) else {}
    return {
        "search_naver": _boolish(raw.get("search_naver"), True),
        "search_partner": _boolish(raw.get("search_partner"), True),
        "contents_naver": _boolish(raw.get("contents_naver"), True),
        "contents_partner": _boolish(raw.get("contents_partner"), True),
    }
def _build_media_target_payload(media_detail: Any) -> Dict[str, Any]:
    detail = _normalize_media_detail(media_detail)
    search: List[str] = []
    contents: List[str] = []
    if detail["search_naver"]:
        search.append("naver")
    if detail["search_partner"]:
        search.append("partner")
    if detail["contents_naver"]:
        contents.append("naver")
    if detail["contents_partner"]:
        contents.append("partner")
    if len(search) == 2 and len(contents) == 2:
        return {
            "type": 1,
            "search": [],
            "contents": [],
            "white": {"media": None, "mediaGroup": None},
            "black": {"media": None, "mediaGroup": None},
        }
    if not search and not contents:
        raise ValueError("세부 매체는 최소 1개 이상 선택해야 합니다.")
    return {
        "type": 2,
        "search": search,
        "contents": contents,
        "black": {"media": [], "mediaGroup": []},
        "white": {"media": None, "mediaGroup": None},
    }
def _update_pc_mobile_target(api_key: str, secret_key: str, cid: str, owner_id: str, media_type: Any):
    pc, mobile = _get_pc_mobile_tuple(media_type)
    res_target, target_obj = _fetch_target_object(api_key, secret_key, cid, owner_id, "PC_MOBILE_TARGET")
    if res_target.status_code == 200 and target_obj and target_obj.get("nccTargetId"):
        payload = {
            "nccTargetId": target_obj.get("nccTargetId"),
            "ownerId": owner_id,
            "targetTp": "PC_MOBILE_TARGET",
            "target": {"pc": pc, "mobile": mobile},
            "delFlag": False,
        }
        res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/targets/{target_obj.get('nccTargetId')}", json_body=payload)
        if res_put.status_code in [200, 201]:
            return True, "PC_MOBILE_TARGET 업데이트 완료"
        fallback_detail = res_put.text
    else:
        fallback_detail = res_target.text if res_target is not None else "PC_MOBILE_TARGET 조회 실패"
    # 하위 호환용 fallback
    res_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{owner_id}")
    if res_get.status_code != 200:
        return False, f"타겟/광고그룹 조회 실패: {fallback_detail}"
    obj = res_get.json() or {}
    obj["pcDevice"], obj["mobileDevice"] = pc, mobile
    res_put2 = _do_req("PUT", api_key, secret_key, cid, f"/ncc/adgroups/{owner_id}", params={"fields": "pcDevice,mobileDevice"}, json_body=obj)
    if res_put2.status_code in [200, 201]:
        return True, "pcDevice/mobileDevice fallback 업데이트 완료"
    return False, f"타겟/광고그룹 업데이트 실패: {fallback_detail} | {res_put2.text}"
def _update_media_target(api_key: str, secret_key: str, cid: str, owner_id: str, media_detail: Any):
    try:
        target_payload = _build_media_target_payload(media_detail)
    except ValueError as e:
        return False, str(e)
    res_target, target_obj = _fetch_target_object(api_key, secret_key, cid, owner_id, "MEDIA_TARGET")
    if res_target.status_code == 200 and target_obj and target_obj.get("nccTargetId"):
        payload = {
            "nccTargetId": target_obj.get("nccTargetId"),
            "ownerId": owner_id,
            "targetTp": "MEDIA_TARGET",
            "target": target_payload,
            "delFlag": False,
        }
        res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/targets/{target_obj.get('nccTargetId')}", json_body=payload)
        if res_put.status_code in [200, 201]:
            return True, "MEDIA_TARGET 업데이트 완료"
        return False, res_put.text
    fallback_detail = res_target.text if res_target is not None else "MEDIA_TARGET 조회 실패"
    return False, f"MEDIA_TARGET 조회 실패: {fallback_detail}"
def _update_non_search_keyword_target(api_key: str, secret_key: str, cid: str, owner_id: str, excluded: bool):
    res_target, target_obj = _fetch_non_search_keyword_target(api_key, secret_key, cid, owner_id)
    if res_target.status_code != 200:
        return False, f"NON_SEARCH_KEYWORD_TARGET 조회 실패: {res_target.text}"
    if not target_obj or not target_obj.get("nccTargetId"):
        return False, "NON_SEARCH_KEYWORD_TARGET 정보를 찾지 못했습니다. 광고센터에서 해당 쇼핑 광고그룹의 제외키워드 탭을 한 번 저장한 뒤 다시 시도해주세요."
    payload = {
        "nccTargetId": target_obj.get("nccTargetId"),
        "ownerId": owner_id,
        "targetTp": "NON_SEARCH_KEYWORD_TARGET",
        "target": {"excluded": bool(excluded)},
        "delFlag": False,
    }
    res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/targets/{target_obj.get('nccTargetId')}", json_body=payload)
    if res_put.status_code in [200, 201]:
        return True, "검색어 없는 경우 광고 노출 제외 설정 완료"
    return False, res_put.text
def _copy_target_payload_from_source_obj(api_key: str, secret_key: str, cid: str, src_target_obj: Dict[str, Any], target_owner_id: str):
    target_type = _normalize_target_type_name((src_target_obj or {}).get("targetTp"))
    if not target_type:
        return False, "타겟 유형 확인 실패"
    src_target = copy.deepcopy((src_target_obj or {}).get("target"))
    if src_target is None:
        return False, f"{target_type} 원본 target 비어 있음"

    res_dst, dst_target_obj = _fetch_target_object(api_key, secret_key, cid, target_owner_id, target_type)
    if res_dst.status_code == 200 and dst_target_obj and dst_target_obj.get("nccTargetId"):
        payload = {
            "customerId": int(cid),
            "nccTargetId": dst_target_obj.get("nccTargetId"),
            "ownerId": str(target_owner_id),
            "targetTp": target_type,
            "target": src_target,
            "delFlag": False,
        }
        res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/targets/{dst_target_obj.get('nccTargetId')}", json_body=payload)
        if res_put.status_code in [200, 201, 204]:
            return True, f"{target_type} 복사 완료"
        return False, f"{target_type} 적용 실패: {res_put.text}"

    if target_type == "PC_MOBILE_TARGET" and isinstance(src_target, dict):
        pc = bool(src_target.get("pc", True))
        mobile = bool(src_target.get("mobile", True))
        if pc and mobile:
            media_type = "ALL"
        elif pc:
            media_type = "PC"
        elif mobile:
            media_type = "MOBILE"
        else:
            return False, "PC_MOBILE_TARGET 원본이 모두 비활성화 상태입니다."
        return _update_pc_mobile_target(api_key, secret_key, cid, target_owner_id, media_type)

    create_payload = {
        "customerId": int(cid),
        "ownerId": str(target_owner_id),
        "targetTp": str(target_type),
        "target": src_target,
        "delFlag": False,
    }
    res_post = _do_req("POST", api_key, secret_key, cid, "/ncc/targets", json_body=create_payload)
    if res_post.status_code in [200, 201, 204]:
        return True, f"{target_type} 복사 완료"
    detail = res_dst.text if res_dst is not None else '알 수 없는 오류'
    return False, f"{target_type} 생성/적용 실패: {res_post.text if res_post is not None else detail}"


def _copy_target_payload_exact(api_key: str, secret_key: str, cid: str, source_owner_id: str, target_owner_id: str, target_type: str):
    target_type = _normalize_target_type_name(target_type)
    res_src, src_target_obj = _fetch_target_object(api_key, secret_key, cid, source_owner_id, target_type)
    if res_src.status_code != 200:
        return False, f"{target_type} 원본 조회 실패: {res_src.text}"
    if not src_target_obj:
        return False, f"{target_type} 원본 설정 없음"
    return _copy_target_payload_from_source_obj(api_key, secret_key, cid, src_target_obj, target_owner_id)
def _fetch_all_target_objects(api_key: str, secret_key: str, cid: str, owner_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/targets", params={"ownerId": owner_id})
    if res.status_code != 200:
        return res, []
    data = res.json() or []
    if isinstance(data, dict):
        data = [data]
    return res, [item for item in data if isinstance(item, dict)]
def _normalize_target_type_name(target_type: Any) -> str:
    return str(target_type or "").strip().upper()
def _is_extra_copy_target_type(target_type: Any) -> bool:
    tp = _normalize_target_type_name(target_type)
    if not tp:
        return False
    if tp in {"PC_MOBILE_TARGET", "MEDIA_TARGET", "RESTRICT_KEYWORD_TARGET", "NON_SEARCH_KEYWORD_TARGET"}:
        return False
    return True

# 광고그룹 복사 시 누락되기 쉬운 타겟 타입을 명시적으로 재조회한다.
# /ncc/targets?ownerId=... 전체 조회가 계정/상품/타겟 탭 상태에 따라 일부 타입을 빠뜨리는 케이스가 있어,
# 원본 광고그룹 상세 targets와 타입별 조회를 같이 병합한다.
ADGROUP_PROFILE_TARGET_TYPES = [
    # v11: 타입 후보를 추측해서 /ncc/targets?types=... 로 찌르면
    # 계정/상품에 따라 4406(The type of Target is invalid) 경고가 반복된다.
    # 따라서 명시 타입 재조회는 하지 않고, 실제 API가 내려준
    # 1) /ncc/targets?ownerId=... 전체 목록
    # 2) /ncc/adgroups/{id} 상세의 targets 배열
    # 안에 존재하는 targetTp만 복사 대상으로 삼는다.
]

TARGET_TYPE_KO_LABELS = {
    "REGIONAL_TARGET": "지역",
    "LOCATION_TARGET": "지역",
    "AREA_TARGET": "지역",
    "GENDER_TARGET": "성별",
    "AGE_TARGET": "연령대",
    "AGE_GROUP_TARGET": "연령대",
    "AGE_BAND_TARGET": "연령대",
    "DEMOGRAPHIC_TARGET": "연령/성별",
    "AGE_GENDER_TARGET": "연령/성별",
    "USER_SEGMENT_TARGET": "이용자 세그먼트",
    "TIME_WEEKLY_TARGET": "요일/시간",
    "PERIOD_TARGET": "기간",
    "MEDIA_TARGET": "매체",
    "PC_MOBILE_TARGET": "PC/Mobile",
}


def _target_type_label(target_type: Any) -> str:
    tp = _normalize_target_type_name(target_type)
    return TARGET_TYPE_KO_LABELS.get(tp, tp or "타겟")


def _compact_json_for_message(value: Any, max_len: int = 260) -> str:
    try:
        txt = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        txt = str(value or "")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt[:max_len] + ("..." if len(txt) > max_len else "")


def _normalize_attr_json_value(value: Any) -> str:
    """Return a stable representation for adgroupAttrJson comparisons.

    Some PowerLink targeting/profile settings are returned only inside
    adgroupAttrJson on newer adgroup objects. The API may return it either as
    a JSON string or as an object depending on account/API version, so compare
    it in a normalized form.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return ""
        try:
            parsed = json.loads(raw)
            return json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return raw
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        return str(value).strip()


def _copy_adgroup_attr_json_settings(api_key: str, secret_key: str, cid: str, source_adgroup_id: str, target_adgroup_id: str):
    """Copy adgroupAttrJson after creating/copying an adgroup.

    지역/성별/연령대처럼 UI에서는 그룹 설정으로 보이지만 /ncc/targets
    목록에 별도 target row가 내려오지 않는 값이 adgroupAttrJson에 들어오는
    계정이 있다. 복사 직후 이 값을 보정 복사하고 재조회로 확인한다.
    """
    res_src, src_obj = _fetch_adgroup_detail(api_key, secret_key, cid, source_adgroup_id)
    if res_src is not None and res_src.status_code != 200:
        return False, f"adgroupAttrJson 원본 조회 실패: {res_src.text}"
    if not isinstance(src_obj, dict) or src_obj.get("adgroupAttrJson") in (None, ""):
        return True, "adgroupAttrJson 원본 설정 없음"
    res_dst, dst_obj = _fetch_adgroup_detail(api_key, secret_key, cid, target_adgroup_id)
    if res_dst is not None and res_dst.status_code != 200:
        return False, f"adgroupAttrJson 대상 조회 실패: {res_dst.text}"
    if not isinstance(dst_obj, dict):
        return False, "adgroupAttrJson 대상 광고그룹 응답 없음"

    src_attr = src_obj.get("adgroupAttrJson")
    src_norm = _normalize_attr_json_value(src_attr)
    if not src_norm:
        return True, "adgroupAttrJson 원본 설정 없음"
    if _normalize_attr_json_value(dst_obj.get("adgroupAttrJson")) == src_norm:
        return True, "adgroupAttrJson 이미 동일"

    attempts = []
    payload = _prepare_adgroup_full_update_obj_for_search_options(dst_obj, cid, {"adgroupAttrJson": src_attr}, sanitized=True)
    res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/adgroups/{target_adgroup_id}", json_body=payload)
    attempts.append(f"full/no-fields: {res_put.status_code} {str(res_put.text or '')[:160]}")
    if res_put.status_code in [200, 201, 204]:
        res_verify, verify_obj = _fetch_adgroup_detail(api_key, secret_key, cid, target_adgroup_id)
        if res_verify is not None and res_verify.status_code == 200 and isinstance(verify_obj, dict):
            if _normalize_attr_json_value(verify_obj.get("adgroupAttrJson")) == src_norm:
                return True, "adgroupAttrJson 복사 완료"
            attempts.append("full/no-fields: 응답 성공이나 재조회값 불일치")

    minimal_payload = {"adgroupAttrJson": src_attr}
    res_put2 = _do_req(
        "PUT", api_key, secret_key, cid, f"/ncc/adgroups/{target_adgroup_id}",
        params={"fields": "adgroupAttrJson"}, json_body=minimal_payload,
    )
    attempts.append(f"fields/adgroupAttrJson: {res_put2.status_code} {str(res_put2.text or '')[:160]}")
    if res_put2.status_code in [200, 201, 204]:
        res_verify, verify_obj = _fetch_adgroup_detail(api_key, secret_key, cid, target_adgroup_id)
        if res_verify is not None and res_verify.status_code == 200 and isinstance(verify_obj, dict):
            if _normalize_attr_json_value(verify_obj.get("adgroupAttrJson")) == src_norm:
                return True, "adgroupAttrJson 복사 완료"
            attempts.append("fields/adgroupAttrJson: 응답 성공이나 재조회값 불일치")
    return False, "adgroupAttrJson 복사 실패/미반영: " + " / ".join(attempts[:4])


def _adgroup_target_diagnostic(adgroup_obj: Dict[str, Any] | None) -> str:
    if not isinstance(adgroup_obj, dict):
        return "광고그룹 상세 응답 없음"
    keys = ", ".join([str(k) for k in list(adgroup_obj.keys())[:40]])
    target_summary = adgroup_obj.get("targetSummary")
    targets = adgroup_obj.get("targets")
    pieces = [f"광고그룹 응답 key: {keys}"]
    if target_summary:
        pieces.append("targetSummary=" + _compact_json_for_message(target_summary, 220))
    if targets:
        pieces.append("targets=" + _compact_json_for_message(targets, 220))
    return " / ".join(pieces)


def _should_surface_target_copy_message(msg: Any) -> bool:
    s = str(msg or "").strip()
    if not s:
        return False
    tokens = [
        "복사 완료", "원본 설정 없음", "원본 타겟 없음", "원본 프로필 타겟 없음", "실패", "미반영",
        "조회", "생성/적용", "비어", "지원", "타겟 확인", "원본 프로필 타겟 확인", "경고", "adgroupAttrJson", "Criterion",
    ]
    return any(tok in s for tok in tokens)



def _target_type_from_obj(row: Dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    for key in ("targetTp", "targetType", "type"):
        value = row.get(key)
        if isinstance(value, str) and value.strip().upper().endswith("_TARGET"):
            return value.strip().upper()
    nested = row.get("target")
    if isinstance(nested, dict):
        for key in ("targetTp", "targetType"):
            value = nested.get(key)
            if isinstance(value, str) and value.strip().upper().endswith("_TARGET"):
                return value.strip().upper()
    return _normalize_target_type_name(row.get("targetTp"))


def _detail_target_type_labels(rows: List[Dict[str, Any]]) -> List[str]:
    labels: List[str] = []
    for row in rows or []:
        tp = _target_type_from_obj(row)
        if tp:
            labels.append(_target_type_label(tp))
    return _unique_keep_order(labels)


def _profile_target_absence_message(adg_obj: Dict[str, Any] | None, detail_rows: List[Dict[str, Any]] | None) -> str:
    labels = _detail_target_type_labels(detail_rows or [])
    diag = _adgroup_target_diagnostic(adg_obj)
    if labels:
        return "원본 프로필 타겟 없음: 상세 targets에는 " + ", ".join(labels) + "만 확인됨. 지역·성별·연령대 targetTp는 응답에 없습니다. " + diag
    return "원본 프로필 타겟 없음/확인 실패: /ncc/targets와 광고그룹 상세 targets에서 지역·성별·연령대 타겟을 찾지 못했습니다. " + diag


def _target_fetch_notice(msg: str) -> bool:
    return "The type of Target is invalid" not in str(msg or "") and '"code":4406' not in str(msg or "")

def _dedupe_target_objects_by_type(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        target_type = _target_type_from_obj(row)
        if not _is_extra_copy_target_type(target_type):
            continue
        if row.get("target") is None:
            continue
        key = target_type
        if key in seen:
            continue
        seen.add(key)
        if not row.get("targetTp") and target_type:
            row = dict(row)
            row["targetTp"] = target_type
        out.append(row)
    return out


def _fetch_copyable_adgroup_target_objects(api_key: str, secret_key: str, cid: str, owner_id: str):
    rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    messages: List[str] = []
    primary_res = _make_fake_response(200, "OK")
    adg_obj: Dict[str, Any] | None = None

    # 1) 공식 Target API 전체 조회. 타입 후보를 붙이지 않는다.
    res_all, all_targets = _fetch_all_target_objects(api_key, secret_key, cid, owner_id)
    primary_res = res_all
    if res_all.status_code == 200:
        rows.extend([row for row in (all_targets or []) if isinstance(row, dict)])
    elif res_all.status_code != 404:
        messages.append(f"전체 타겟 조회 실패: {res_all.text}")

    # 2) 광고그룹 상세 응답의 targets 배열 fallback.
    try:
        res_adg, adg_obj = _fetch_adgroup_detail(api_key, secret_key, cid, owner_id)
        if res_adg.status_code == 200 and isinstance(adg_obj, dict):
            raw_detail_targets = adg_obj.get("targets")
            if isinstance(raw_detail_targets, list):
                detail_rows = [row for row in raw_detail_targets if isinstance(row, dict)]
            elif isinstance(raw_detail_targets, dict):
                detail_rows = [raw_detail_targets]
            rows.extend(detail_rows)
        elif res_adg.status_code not in {200, 404}:
            messages.append(f"광고그룹 상세 조회 참고: {res_adg.text[:180]}")
    except Exception as e:
        messages.append(f"광고그룹 상세 targets 조회 예외: {e}")

    # 3) 실제 응답에서 발견된 프로필 targetTp만 보강 조회한다.
    seen_profile_types = _unique_keep_order([
        _target_type_from_obj(row) for row in rows
        if _is_extra_copy_target_type(_target_type_from_obj(row))
    ])
    for target_type in seen_profile_types:
        try:
            res_one, target_obj = _fetch_target_object(api_key, secret_key, cid, owner_id, target_type)
        except Exception as e:
            messages.append(f"{_target_type_label(target_type)} 타겟 조회 예외: {e}")
            continue
        if res_one.status_code == 200 and target_obj:
            rows.append(target_obj)
        elif res_one.status_code not in {200, 404}:
            txt = str(res_one.text or "").strip()
            if txt and len(messages) < 6 and _target_fetch_notice(txt):
                messages.append(f"{_target_type_label(target_type)} 타겟 조회 참고: {txt[:180]}")

    copyable = _dedupe_target_objects_by_type(rows)
    if res_all.status_code != 200 and copyable:
        primary_res = _make_fake_response(200, "상세 fallback으로 타겟 확인")
    if not copyable:
        messages.append(_profile_target_absence_message(adg_obj, detail_rows))
    else:
        labels = [_target_type_label(_target_type_from_obj(row)) for row in copyable]
        messages.append("원본 프로필 타겟 확인: " + ", ".join(_unique_keep_order(labels)))
    return primary_res, copyable, messages

def _copy_profile_targets_exact(api_key: str, secret_key: str, cid: str, source_owner_id: str, target_owner_id: str):
    messages: List[str] = []
    overall_ok = True
    res_src, src_targets, fetch_messages = _fetch_copyable_adgroup_target_objects(api_key, secret_key, cid, source_owner_id)
    if fetch_messages:
        messages.extend([m for m in fetch_messages if m][:8])
    if res_src.status_code != 200 and not src_targets:
        return False, messages + [f"추가 타겟 조회 실패: {res_src.text}"]
    if not src_targets:
        # 이전에는 여기서 True, [] 로 끝나 복사 화면에서 조용히 누락됐다.
        # 복사/일괄적용 모두 원본 타겟을 못 잡은 사실을 반드시 표시한다.
        return False, messages or ["추가 타겟 원본 설정 없음"]
    copied_types: List[str] = []
    for src_target in src_targets:
        target_type = _target_type_from_obj(src_target)
        if not _is_extra_copy_target_type(target_type):
            continue
        ok, msg = _copy_target_payload_from_source_obj(api_key, secret_key, cid, src_target, target_owner_id)
        if ok:
            copied_types.append(target_type)
        else:
            overall_ok = False
            messages.append(msg)
    if copied_types:
        copied_labels = [_target_type_label(tp) for tp in copied_types]
        messages.append("추가 타겟 복사 완료: " + ", ".join(_unique_keep_order(copied_labels)))
    elif overall_ok:
        overall_ok = False
        messages.append("추가 타겟 원본 설정 없음")
    return overall_ok, messages
def _extract_schedule_codes_from_payload(payload: Any) -> List[str]:
    found: List[str] = []
    seen = set()
    day_token_to_num = {
        "MON": 1, "MONDAY": 1, "MO": 1, "월": 1,
        "TUE": 2, "TUESDAY": 2, "TU": 2, "화": 2,
        "WED": 3, "WEDNESDAY": 3, "WE": 3, "수": 3,
        "THU": 4, "THURSDAY": 4, "TH": 4, "목": 4,
        "FRI": 5, "FRIDAY": 5, "FR": 5, "금": 5,
        "SAT": 6, "SATURDAY": 6, "SA": 6, "토": 6,
        "SUN": 7, "SUNDAY": 7, "SU": 7, "일": 7,
    }

    def _append_code(code: Any):
        value = str(code or "").strip().upper()
        if re.fullmatch(r"SD[A-Z]{2}\d{4}", value) and value not in seen:
            seen.add(value)
            found.append(value)

    def _coerce_hour(value: Any):
        if value is None:
            return None
        if isinstance(value, str):
            m = re.search(r"(\d{1,2})", value)
            if not m:
                return None
            value = m.group(1)
        try:
            n = int(value)
        except Exception:
            return None
        return n if 0 <= n <= 24 else None

    def _coerce_day_num(value: Any):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            n = int(value)
            return n if n in DAY_NUM_TO_CODE else None
        token = str(value or "").strip().upper()
        if not token:
            return None
        if token.isdigit():
            n = int(token)
            return n if n in DAY_NUM_TO_CODE else None
        return day_token_to_num.get(token)

    def visit(node: Any):
        if isinstance(node, dict):
            for k in ("dictionaryCode", "code"):
                if k in node:
                    _append_code(node.get(k))
            day_num = None
            for dk in ("day", "dayOfWeek", "weekDay", "weekday", "dayCode"):
                if dk in node:
                    day_num = _coerce_day_num(node.get(dk))
                    if day_num:
                        break
            start_h = None
            for sk in ("start", "startHour", "startTm", "startTime", "beginHour"):
                if sk in node:
                    start_h = _coerce_hour(node.get(sk))
                    if start_h is not None:
                        break
            end_h = None
            for ek in ("end", "endHour", "endTm", "endTime", "finishHour"):
                if ek in node:
                    end_h = _coerce_hour(node.get(ek))
                    if end_h is not None:
                        break
            if day_num and start_h is not None and end_h is not None and 0 <= start_h <= 23 and 1 <= end_h <= 24 and end_h > start_h:
                day_code = DAY_NUM_TO_CODE.get(day_num)
                if day_code:
                    for hour in range(start_h, end_h):
                        _append_code(f"SD{day_code}{hour:02d}{hour + 1:02d}")
            for value in node.values():
                visit(value)
            return
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if isinstance(node, str):
            _append_code(node)

    visit(payload)
    return found

def _fetch_schedule_entries(api_key: str, secret_key: str, cid: str, owner_id: str):
    owner_id = str(owner_id or "").strip()
    if not owner_id:
        return _make_fake_response(400, "ownerId가 없습니다."), []

    # v14: /ncc/criterion/{ownerId}/SD 단일 GET은 계정/환경에 따라 405가 날 수 있다.
    # AG/GN/RL 조회에 성공한 criterion fallback과 동일하게 여러 조회 경로를 순차 시도한다.
    attempts: List[str] = []
    request_variants = [
        (f"/ncc/criterion/{owner_id}/SD", None),
        (f"/ncc/criterion/{owner_id}", {"type": "SD"}),
        ("/ncc/criterion", {"ownerId": owner_id, "type": "SD"}),
        (f"/ncc/criteria/{owner_id}/SD", None),
        ("/ncc/criteria", {"ownerId": owner_id, "type": "SD"}),
    ]
    first_res = None
    saw_not_found = False
    for uri, params in request_variants:
        res = _do_req("GET", api_key, secret_key, cid, uri, params=params)
        if first_res is None:
            first_res = res
        attempts.append(f"GET {uri}{'?' + urlencode(params) if params else ''} -> {res.status_code}")
        if res.status_code in {404, 204}:
            saw_not_found = True
            continue
        if res.status_code != 200:
            continue
        try:
            data = res.json() or []
        except Exception:
            data = []
        if isinstance(data, dict):
            for list_key in ("data", "items", "rows", "contents"):
                if isinstance(data.get(list_key), list):
                    data = data.get(list_key)
                    break
            else:
                data = [data]
        rows = [item for item in data if isinstance(item, dict)]
        fake = _make_fake_response(200, "OK")
        try:
            setattr(fake, "debug_attempts", attempts)
        except Exception:
            pass
        return fake, rows

    # 마지막 fallback: 광고그룹 상세/속성 안에 SD 코드가 노출되는 경우만 보정한다.
    res_adg, adgroup_obj = _fetch_adgroup_detail(api_key, secret_key, cid, owner_id)
    if res_adg.status_code == 200 and isinstance(adgroup_obj, dict):
        fallback_codes = _extract_schedule_codes_from_payload(adgroup_obj)
        if fallback_codes:
            fallback_rows = [{"dictionaryCode": code, "bidWeight": 100} for code in fallback_codes]
            fake = _make_fake_response(200, "SCHEDULE adgroup fallback")
            try:
                setattr(fake, "debug_attempts", attempts + ["adgroup detail fallback -> codes"])
            except Exception:
                pass
            return fake, fallback_rows

    if saw_not_found:
        fallback = _make_fake_response(404, "SCHEDULE 원본 설정 없음")
    else:
        fallback = first_res or _make_fake_response(404, "SCHEDULE criterion 조회 결과 없음")
    try:
        setattr(fallback, "debug_attempts", attempts)
        if getattr(fallback, "status_code", None) not in {200, 404} and attempts:
            fallback._content = ("SCHEDULE 조회 실패: " + " / ".join(attempts[:5])).encode("utf-8")
    except Exception:
        pass
    return fallback, []

def _copy_schedule_criterion_exact(api_key: str, secret_key: str, cid: str, source_owner_id: str, target_owner_id: str):
    res_src, src_rows = _fetch_schedule_entries(api_key, secret_key, cid, source_owner_id)
    if res_src.status_code == 404:
        return False, "SCHEDULE 원본 설정 없음"
    if res_src.status_code != 200:
        return False, f"SCHEDULE 원본 조회 실패: {res_src.text}"
    codes: List[str] = []
    bid_weight_map: Dict[str, int] = {}
    for row in (src_rows or []):
        code = str(row.get("dictionaryCode") or row.get("code") or "").strip()
        if not code:
            continue
        codes.append(code)
        try:
            bid_weight = int(row.get("bidWeight") or 100)
        except Exception:
            bid_weight = 100
        bid_weight_map[code] = bid_weight
    codes = _unique_keep_order(codes)
    if not codes:
        return False, "SCHEDULE 원본 설정 없음"
    body = [{"customerId": int(cid), "ownerId": str(target_owner_id), "dictionaryCode": code, "type": "SD"} for code in codes]
    res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/criterion/{target_owner_id}/SD", json_body=body)
    if res_put.status_code != 200:
        return False, f"SCHEDULE 적용 실패: {res_put.text}"
    weight_groups: Dict[int, List[str]] = defaultdict(list)
    for code in codes:
        try:
            weight = int(bid_weight_map.get(code, 100) or 100)
        except Exception:
            weight = 100
        weight_groups[weight].append(code)
    for bid_weight, group_codes in weight_groups.items():
        if int(bid_weight) == 100 or not group_codes:
            continue
        for i in range(0, len(group_codes), 50):
            chunk = group_codes[i:i + 50]
            res_bw = _do_req(
                "PUT",
                api_key,
                secret_key,
                cid,
                f"/ncc/criterion/{target_owner_id}/bidWeight",
                params={"codes": ",".join(chunk), "bidWeight": int(bid_weight)},
            )
            if res_bw.status_code != 200:
                return False, f"SCHEDULE 입찰가중치 적용 실패: {res_bw.text}"
    # 적용 후 같은 조회 경로로 재조회해 실제 코드가 들어갔는지 확인한다.
    res_check, check_rows = _fetch_schedule_entries(api_key, secret_key, cid, target_owner_id)
    if res_check.status_code == 200:
        check_codes = _unique_keep_order([str((row or {}).get("dictionaryCode") or (row or {}).get("code") or "").strip() for row in (check_rows or [])])
        missing = [code for code in codes if code not in check_codes]
        if missing:
            return False, f"SCHEDULE 적용 후 미반영: 누락 {len(missing)}개 / 예: {', '.join(missing[:5])}"
    return True, f"SCHEDULE 복사 완료: 요일/시간 {len(codes)}개"

PROFILE_CRITERION_TYPE_CANDIDATES: List[Tuple[str, str]] = [
    # 네이버 Criterion Master 실제 type 기준: SD, AG, GN, RL, RP, AD.
    # v12에서는 지역을 RG/RT로 추측해서 원본 없음으로 빠질 수 있었다.
    # 지역 타겟은 RL/RP, 성별은 GN, 연령은 AG를 우선 사용하고, AD는
    # 잠재고객/이용자 세그먼트류 후보로 함께 보정 복사한다.
    ("AG", "연령대"),
    ("GN", "성별"),
    ("RL", "지역"),
    ("RP", "반경/지역"),
    ("AD", "이용자 세그먼트"),
]

PROFILE_CRITERION_CATEGORY_TYPES: Dict[str, List[str]] = {
    "age": ["AG"],
    "gender": ["GN"],
    "region": ["RL", "RP"],
    "segment": ["AD"],
}

def _normalize_profile_criterion_types(types: Any = None) -> List[str]:
    """Return valid profile Criterion type codes in the supported order."""
    all_types = [tp for tp, _ in PROFILE_CRITERION_TYPE_CANDIDATES]
    if types is None:
        return list(all_types)
    if isinstance(types, str):
        raw = [types]
    else:
        try:
            raw = list(types or [])
        except Exception:
            raw = []
    expanded: List[str] = []
    for item in raw:
        token = str(item or "").strip()
        if not token:
            continue
        low = token.lower()
        if low in PROFILE_CRITERION_CATEGORY_TYPES:
            expanded.extend(PROFILE_CRITERION_CATEGORY_TYPES[low])
        else:
            expanded.append(token.upper())
    valid = set(all_types)
    return _unique_keep_order([tp for tp in expanded if tp in valid])

def _fetch_criterion_entries(api_key: str, secret_key: str, cid: str, owner_id: str, criterion_type: str):
    owner_id = str(owner_id or "").strip()
    criterion_type = str(criterion_type or "").strip().upper()
    if not owner_id or not criterion_type:
        return _make_fake_response(400, "ownerId 또는 criterion type이 없습니다."), []

    attempts: List[str] = []
    request_variants = [
        (f"/ncc/criterion/{owner_id}/{criterion_type}", None),
        (f"/ncc/criterion/{owner_id}", {"type": criterion_type}),
        ("/ncc/criterion", {"ownerId": owner_id, "type": criterion_type}),
        (f"/ncc/criteria/{owner_id}/{criterion_type}", None),
        ("/ncc/criteria", {"ownerId": owner_id, "type": criterion_type}),
    ]
    first_res = None
    for uri, params in request_variants:
        res = _do_req("GET", api_key, secret_key, cid, uri, params=params)
        if first_res is None:
            first_res = res
        attempts.append(f"GET {uri}{'?' + urlencode(params) if params else ''} -> {res.status_code}")
        if res.status_code != 200:
            continue
        try:
            data = res.json() or []
        except Exception:
            data = []
        if isinstance(data, dict):
            for list_key in ("data", "items", "rows", "contents"):
                if isinstance(data.get(list_key), list):
                    data = data.get(list_key)
                    break
            else:
                data = [data]
        rows = [item for item in data if isinstance(item, dict)]
        fake = _make_fake_response(200, "OK")
        setattr(fake, "debug_attempts", attempts)
        return fake, rows
    fallback = first_res or _make_fake_response(404, "criterion 조회 결과 없음")
    try:
        setattr(fallback, "debug_attempts", attempts)
    except Exception:
        pass
    return fallback, []

def _extract_criterion_codes_from_payload(payload: Any, prefixes: List[str]) -> Dict[str, List[str]]:
    """Extract criterion dictionary codes from adgroup detail/attr JSON fallbacks.

    Naver returns some targeting state in adgroup detail fields rather than as
    /ncc/targets rows. We scan only known Criterion prefixes to avoid copying
    unrelated ids.
    """
    wanted = [str(p or "").upper() for p in (prefixes or []) if str(p or "").strip()]
    out: Dict[str, List[str]] = {p: [] for p in wanted}
    seen = set()
    if not wanted:
        return out
    pattern = re.compile(r"\b(" + "|".join(re.escape(p) for p in wanted) + r")[-_A-Z0-9]*\d{1,}\b", re.I)

    def _scan_text(text: Any):
        if text is None:
            return
        txt = str(text)
        if not txt:
            return
        for m in pattern.finditer(txt):
            code = m.group(0).strip().upper()
            prefix = m.group(1).strip().upper()
            key = (prefix, code)
            if prefix in out and key not in seen:
                seen.add(key)
                out[prefix].append(code)

    def visit(node: Any, depth: int = 0):
        if depth > 8:
            return
        if isinstance(node, dict):
            for k, v in node.items():
                if k in {"dictionaryCode", "code", "criterionCode", "targetCode"}:
                    _scan_text(v)
                else:
                    _scan_text(k)
                    if isinstance(v, (str, int, float)):
                        _scan_text(v)
                visit(v, depth + 1)
            return
        if isinstance(node, list):
            for item in node:
                visit(item, depth + 1)
            return
        if isinstance(node, str):
            raw = node.strip()
            _scan_text(raw)
            if raw and (raw.startswith("{") or raw.startswith("[")):
                try:
                    visit(json.loads(raw), depth + 1)
                except Exception:
                    pass

    visit(payload)
    return out


def _criterion_rows_to_weight_map(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows or []:
        code = str((row or {}).get("dictionaryCode") or (row or {}).get("code") or "").strip().upper()
        if not code:
            continue
        try:
            out[code] = int((row or {}).get("bidWeight") or 100)
        except Exception:
            out[code] = 100
    return out


def _row_first_present(row: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for key in keys:
        if isinstance(row, dict) and key in row and row.get(key) is not None:
            return row.get(key)
    return default


def _criterion_rows_to_state_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        code = str(_row_first_present(row, ["dictionaryCode", "code", "criterionCode", "targetCode"], "") or "").strip().upper()
        if not code:
            continue
        try:
            bid_weight = int(_row_first_present(row, ["bidWeight", "bid_weight", "weight"], 100) or 100)
        except Exception:
            bid_weight = 100
        try:
            negative = int(_row_first_present(row, ["negative", "negativeYn", "exclude", "excluded"], 0) or 0)
        except Exception:
            negative = 0
        try:
            onoff = int(_row_first_present(row, ["onOff", "onoff", "enabled", "enable", "on"], 1) or 1)
        except Exception:
            onoff = 1
        state = {
            "dictionaryCode": code,
            "bidWeight": bid_weight,
            "negative": negative,
            "onOff": onoff,
        }
        add_info = _row_first_present(row, ["additionalInfo", "additional_info", "attr", "target", "value"], None)
        if add_info not in (None, ""):
            state["additionalInfo"] = add_info
        out[code] = state
    return out


def _put_criterion_state_map(api_key: str, secret_key: str, cid: str, owner_id: str, criterion_type: str, final_map: Dict[str, Dict[str, Any]]):
    owner_id = str(owner_id or "").strip()
    criterion_type = str(criterion_type or "").strip().upper()
    states = [v for _, v in sorted((final_map or {}).items()) if isinstance(v, dict) and str(v.get("dictionaryCode") or "").strip()]
    if not states:
        return False, "criterion_empty", "적용할 criterion code가 없습니다."
    body = []
    for state in states:
        item = {
            "customerId": int(cid),
            "ownerId": owner_id,
            "dictionaryCode": str(state.get("dictionaryCode") or "").strip().upper(),
            "type": criterion_type,
        }
        if "negative" in state:
            item["negative"] = int(state.get("negative") or 0)
        if "onOff" in state:
            item["onOff"] = int(state.get("onOff") or 1)
        if state.get("additionalInfo") not in (None, ""):
            item["additionalInfo"] = state.get("additionalInfo")
        body.append(item)

    put_variants = [
        (f"/ncc/criterion/{owner_id}/{criterion_type}", None),
        (f"/ncc/criterion/{owner_id}", {"type": criterion_type}),
        ("/ncc/criterion", {"ownerId": owner_id, "type": criterion_type}),
        (f"/ncc/criteria/{owner_id}/{criterion_type}", None),
        ("/ncc/criteria", {"ownerId": owner_id, "type": criterion_type}),
    ]
    attempts: List[str] = []
    put_ok = False
    last_text = ""
    for uri, params in put_variants:
        put_res = _do_req("PUT", api_key, secret_key, cid, uri, params=params, json_body=body)
        attempts.append(f"PUT {uri}{'?' + urlencode(params) if params else ''} -> {put_res.status_code}")
        last_text = str(put_res.text or "")[:220]
        if put_res.status_code in {200, 201, 204}:
            put_ok = True
            break
    if not put_ok:
        return False, "criterion_put", (" / ".join(attempts[:5]) + (f" | {last_text}" if last_text else ""))

    weight_map: Dict[int, List[str]] = {}
    for state in states:
        try:
            weight = int(state.get("bidWeight", 100) or 100)
        except Exception:
            weight = 100
        weight_map.setdefault(weight, []).append(str(state.get("dictionaryCode") or "").strip().upper())
    for weight, weight_codes in weight_map.items():
        if int(weight) == 100 or not weight_codes:
            continue
        for i in range(0, len(weight_codes), 50):
            chunk = weight_codes[i:i + 50]
            bw_res = _do_req(
                "PUT", api_key, secret_key, cid,
                f"/ncc/criterion/{owner_id}/bidWeight",
                params={"codes": ",".join(chunk), "bidWeight": int(weight)},
            )
            if bw_res.status_code not in {200, 201, 204}:
                return False, "criterion_bid_weight", bw_res.text
    return True, "", ""


def _put_criterion_weight_map(api_key: str, secret_key: str, cid: str, owner_id: str, criterion_type: str, final_map: Dict[str, int]):
    owner_id = str(owner_id or "").strip()
    criterion_type = str(criterion_type or "").strip().upper()
    codes = _unique_keep_order([str(c or "").strip().upper() for c in (final_map or {}).keys() if str(c or "").strip()])
    if not codes:
        return False, "criterion_empty", "적용할 criterion code가 없습니다."
    body = [{"customerId": int(cid), "ownerId": owner_id, "dictionaryCode": c, "type": criterion_type} for c in codes]
    put_res = _do_req("PUT", api_key, secret_key, cid, f"/ncc/criterion/{owner_id}/{criterion_type}", json_body=body)
    if put_res.status_code not in {200, 201, 204}:
        return False, "criterion_put", put_res.text
    weight_map: Dict[int, List[str]] = {}
    for code in codes:
        try:
            weight = int((final_map or {}).get(code, 100) or 100)
        except Exception:
            weight = 100
        weight_map.setdefault(weight, []).append(code)
    for weight, weight_codes in weight_map.items():
        if int(weight) == 100 or not weight_codes:
            continue
        for i in range(0, len(weight_codes), 50):
            chunk = weight_codes[i:i + 50]
            bw_res = _do_req(
                "PUT", api_key, secret_key, cid,
                f"/ncc/criterion/{owner_id}/bidWeight",
                params={"codes": ",".join(chunk), "bidWeight": int(weight)},
            )
            if bw_res.status_code not in {200, 201, 204}:
                return False, "criterion_bid_weight", bw_res.text
    return True, "", ""


def _copy_profile_criteria_exact(api_key: str, secret_key: str, cid: str, source_owner_id: str, target_owner_id: str, include_types: Any = None):
    """Copy profile targeting stored as Criterion rows (age/gender/region)."""
    messages: List[str] = []
    copied_labels: List[str] = []
    failed_labels: List[str] = []
    lookup_notes: List[str] = []
    source_detail: Dict[str, Any] | None = None
    fallback_codes: Dict[str, List[str]] = {}
    selected_types = _normalize_profile_criterion_types(include_types)
    if not selected_types:
        return False, ["Criterion 타겟 적용 항목 없음"]
    selected_set = set(selected_types)
    selected_candidates = [(tp, label) for tp, label in PROFILE_CRITERION_TYPE_CANDIDATES if tp in selected_set]
    prefixes = _unique_keep_order([tp for tp, _ in selected_candidates])
    try:
        res_detail, source_detail = _fetch_adgroup_detail(api_key, secret_key, cid, source_owner_id)
        if res_detail.status_code == 200 and isinstance(source_detail, dict):
            fallback_codes = _extract_criterion_codes_from_payload(source_detail, prefixes)
    except Exception as e:
        messages.append(f"Criterion 상세 fallback 조회 예외: {e}")

    for criterion_type, label in selected_candidates:
        res_src, rows = _fetch_criterion_entries(api_key, secret_key, cid, source_owner_id, criterion_type)
        state_map = _criterion_rows_to_state_map(rows or [])
        debug_attempts = getattr(res_src, "debug_attempts", []) or []
        note = f"{label}({criterion_type}) status={getattr(res_src, 'status_code', '?')}, rows={len(rows or [])}"
        if debug_attempts and (not rows):
            note += " [" + "; ".join(debug_attempts[:2]) + "]"
        lookup_notes.append(note)
        if not state_map:
            for code in fallback_codes.get(criterion_type, []) or []:
                code = str(code).strip().upper()
                if code:
                    state_map[code] = {"dictionaryCode": code, "bidWeight": 100, "negative": 0, "onOff": 1}
        if not state_map:
            continue
        ok_put, step, detail = _put_criterion_state_map(api_key, secret_key, cid, target_owner_id, criterion_type, state_map)
        if ok_put:
            copied_labels.append(f"{label}({criterion_type}) {len(state_map)}개")
        else:
            failed_labels.append(f"{label}({criterion_type}) {step}: {str(detail or '')[:220]}")

    if copied_labels:
        messages.append("Criterion 타겟 복사 완료: " + ", ".join(_unique_keep_order(copied_labels)))
    if failed_labels:
        messages.extend(["Criterion 타겟 적용 실패: " + msg for msg in failed_labels[:8]])
    if not copied_labels and not failed_labels:
        messages.append("Criterion 타겟 원본 없음: 연령대/성별/지역/세그먼트 코드 미확인 · " + " / ".join(lookup_notes[:5]))
    elif failed_labels and lookup_notes:
        messages.append("Criterion 조회 참고: " + " / ".join(lookup_notes[:5]))
    return bool(copied_labels) and not bool(failed_labels), messages

def _copy_adgroup_extra_target_settings(api_key: str, secret_key: str, cid: str, source_adgroup_id: str, target_adgroup_id: str):
    # 캠페인/그룹 복사 후 원본 광고그룹의 프로필 타겟(지역/성별/연령대 등)을 보정 복사한다.
    # 1) /ncc/targets 기반 타겟 복사
    # 2) /ncc/criterion 기반 연령대/성별/지역 타겟 복사
    # 3) 신규/숨김 그룹 속성(adgroupAttrJson) 보정 복사
    # 4) SCHEDULE은 GET 미지원 계정이 있어 실패가 다른 타겟 복사를 가리지 않도록 별도 참고 메시지로만 처리
    messages: List[str] = []
    applied_any = False
    hard_fail = False

    ok_targets, target_msgs = _copy_profile_targets_exact(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id)
    if target_msgs:
        messages.extend(target_msgs)
    if ok_targets:
        applied_any = True
    elif target_msgs and not any("원본" in str(m) and ("없음" in str(m) or "미확인" in str(m)) for m in target_msgs):
        hard_fail = True

    ok_criteria, criteria_msgs = _copy_profile_criteria_exact(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id)
    if criteria_msgs:
        messages.extend(criteria_msgs)
    if ok_criteria:
        applied_any = True
    elif criteria_msgs and any("적용 실패" in str(m) for m in criteria_msgs):
        hard_fail = True

    ok_attr, msg_attr = _copy_adgroup_attr_json_settings(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id)
    if msg_attr and msg_attr != "adgroupAttrJson 원본 설정 없음":
        messages.append(msg_attr)
    if ok_attr and msg_attr not in {"adgroupAttrJson 원본 설정 없음", "adgroupAttrJson 이미 동일"}:
        applied_any = True
    elif not ok_attr:
        hard_fail = True

    ok_schedule, msg_schedule = _copy_schedule_criterion_exact(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id)
    if msg_schedule and msg_schedule not in {"", "SCHEDULE 원본 설정 없음"}:
        if "405" in str(msg_schedule) or "Method Not Allowed" in str(msg_schedule):
            messages.append("SCHEDULE 복사 제외: API GET 미지원(405)")
        else:
            messages.append(msg_schedule)
            if ok_schedule:
                applied_any = True
            else:
                hard_fail = True
    return applied_any and not hard_fail, _unique_keep_order(messages)

def _resolve_bulk_target_adgroup_ids(api_key: str, secret_key: str, cid: str, target_scope: str, campaign_ids: List[str], adgroup_ids: List[str]):
    scope = str(target_scope or "adgroup").strip().lower()
    if scope == "campaign":
        resolved: List[str] = []
        warnings: List[str] = []
        for camp_id in _unique_keep_order([str(x).strip() for x in (campaign_ids or []) if str(x).strip()]):
            r_adgs, rows = _fetch_adgroups(api_key, secret_key, cid, camp_id, enrich_media=False)
            if r_adgs.status_code == 200:
                resolved.extend([str((row or {}).get("id") or (row or {}).get("nccAdgroupId") or "").strip() for row in (rows or [])])
            else:
                warnings.append(f"[{camp_id}] 하위 광고그룹 조회 실패: {r_adgs.text}")
        return _unique_keep_order([x for x in resolved if x]), warnings
    return _unique_keep_order([str(x).strip() for x in (adgroup_ids or []) if str(x).strip()]), []
def _copy_adgroup_extra_targets_only(
    api_key: str,
    secret_key: str,
    cid: str,
    source_adgroup_id: str,
    target_adgroup_id: str,
    include_criterion_types: Any = None,
    include_target_rows: bool = False,
    include_attr_json: bool = True,
):
    # 타겟 설정 일괄 적용 전용.
    # v15부터 연령대/성별/지역/세그먼트/시간대를 각각 선택 적용할 수 있도록
    # Criterion 타입(AG/GN/RL/RP/AD)을 명시적으로 필터링한다.
    messages: List[str] = []
    applied_any = False
    hard_fail = False

    if include_target_rows:
        ok_targets, target_msgs = _copy_profile_targets_exact(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id)
        if target_msgs:
            messages.extend(target_msgs)
        if ok_targets:
            applied_any = True
        elif target_msgs and not any("원본" in str(m) and ("없음" in str(m) or "미확인" in str(m)) for m in target_msgs):
            hard_fail = True

    selected_types = _normalize_profile_criterion_types(include_criterion_types)
    if selected_types:
        ok_criteria, criteria_msgs = _copy_profile_criteria_exact(
            api_key, secret_key, cid, source_adgroup_id, target_adgroup_id, include_types=selected_types
        )
        if criteria_msgs:
            messages.extend(criteria_msgs)
        if ok_criteria:
            applied_any = True
        elif criteria_msgs and any("적용 실패" in str(m) or "미반영" in str(m) for m in criteria_msgs):
            hard_fail = True

    if include_attr_json:
        ok_attr, msg_attr = _copy_adgroup_attr_json_settings(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id)
        if msg_attr and msg_attr != "adgroupAttrJson 원본 설정 없음":
            messages.append(msg_attr)
        if ok_attr and msg_attr not in {"adgroupAttrJson 원본 설정 없음", "adgroupAttrJson 이미 동일"}:
            applied_any = True
        elif not ok_attr:
            hard_fail = True

    if not messages:
        messages.append("선택한 프로필 타겟 원본 없음")
    return applied_any and not hard_fail, _unique_keep_order(messages)

def _copy_adgroup_search_option_settings(api_key: str, secret_key: str, cid: str, source_adgroup_id: str, target_adgroup_id: str):
    """Copy current PowerLink search options from one adgroup to another.

    2026-04 실제 응답 기준 확장검색 필드는 useExpSearch / expSearchBudgetRatio다.
    신규 광고그룹 생성 시 POST payload에 포함해도 서버에서 기본값으로 재계산될 수 있으므로,
    그룹/캠페인 복사 후 한 번 더 no-fields 전체 update로 원본값을 맞춘다.
    """
    try:
        res_get, src_obj = _fetch_adgroup_detail(api_key, secret_key, cid, source_adgroup_id)
    except Exception as e:
        return False, f"검색옵션 원본 조회 실패: {e}"
    if res_get.status_code != 200 or not isinstance(src_obj, dict):
        detail = res_get.text if res_get is not None else "광고그룹 조회 실패"
        return False, f"검색옵션 원본 조회 실패: {detail}"
    if str(src_obj.get("adgroupType") or "").upper() != "WEB_SITE":
        return True, "파워링크 그룹이 아니어서 검색옵션 복사 건너뜀"

    exp_key, exp_value = _read_expanded_search_from_obj(src_obj)
    ratio_key, ratio_value = _read_expanded_budget_ratio_from_obj(src_obj)

    if exp_key is not None:
        update_values: Dict[str, Any] = {exp_key: exp_value}
        if ratio_key is not None:
            update_values[ratio_key] = ratio_value
        try:
            res_tgt, target_obj = _fetch_adgroup_detail(api_key, secret_key, cid, target_adgroup_id)
        except Exception as e:
            return False, f"검색옵션 대상 조회 실패: {e}"
        if res_tgt.status_code != 200 or not isinstance(target_obj, dict):
            detail = res_tgt.text if res_tgt is not None else "광고그룹 조회 실패"
            return False, f"검색옵션 대상 조회 실패: {detail}"
        if str(target_obj.get("adgroupType") or "").upper() != "WEB_SITE":
            return True, "대상 그룹이 파워링크가 아니어서 검색옵션 복사 건너뜀"

        # 실제 적용 성공이 확인된 방식: fields 없이 전체 광고그룹 update(no-fields).
        # target_obj를 베이스로 써야 신규 그룹 ID/캠페인/채널 정보가 보존된다.
        res_put = _put_adgroup_expanded_search_candidate(
            api_key, secret_key, cid, target_adgroup_id, target_obj, None, update_values, full_payload=True
        )
        if res_put.status_code not in [200, 201, 204]:
            # no-fields가 실패하면 기존 변경 함수의 후보 전략으로 한 번 더 보정한다.
            expected_enabled = _normalize_bool_for_api(exp_value, False)
            fallback_ratio = ratio_value if ratio_key is not None else None
            ok, msg = _update_adgroup_search_options(
                api_key, secret_key, cid, target_adgroup_id,
                use_keyword_plus=expected_enabled,
                keyword_plus_weight=fallback_ratio,
                use_close_variant=None,
            )
            if ok:
                return True, f"검색옵션 복사 완료(fallback): {msg}"
            return False, f"검색옵션 복사 실패(no-fields): {res_put.text} / fallback: {msg}"

        res_verify, verify_obj = _fetch_adgroup_detail(api_key, secret_key, cid, target_adgroup_id)
        if res_verify.status_code != 200 or not isinstance(verify_obj, dict):
            detail = res_verify.text if res_verify is not None else "재조회 실패"
            return False, f"검색옵션 복사 후 재조회 실패: {detail}"
        check_key, check_value = _read_expanded_search_from_obj(verify_obj)
        if check_key is None:
            keys_preview = ", ".join(list((verify_obj or {}).keys())[:35])
            return False, f"검색옵션 복사 후 확인 필드 없음. 확인된 key: {keys_preview}"
        expected_enabled = _normalize_bool_for_api(exp_value, False)
        if not _same_expanded_search_value(check_value, expected_enabled):
            return False, f"검색옵션 복사 미반영({check_key}: 원본 {exp_value} / 확인 {check_value})"
        if ratio_key is not None:
            verify_ratio_key, verify_ratio_value = _read_expanded_budget_ratio_from_obj(verify_obj)
            if verify_ratio_key is None:
                return False, f"확장검색 예산비율 확인 필드 없음({ratio_key})"
            try:
                same_ratio = int(float(verify_ratio_value)) == int(float(ratio_value))
            except Exception:
                same_ratio = str(verify_ratio_value) == str(ratio_value)
            if not same_ratio:
                return False, f"확장검색 예산비율 복사 미반영({verify_ratio_key}: 원본 {ratio_value} / 확인 {verify_ratio_value})"
        ratio_msg = f", {ratio_key}={ratio_value}" if ratio_key is not None else ""
        return True, f"검색옵션 복사 완료({exp_key}={exp_value}{ratio_msg})"

    # 구형 응답 필드가 남아 있는 계정용 호환 fallback.
    use_keyword_plus = src_obj.get("useKeywordPlus") if "useKeywordPlus" in src_obj else None
    keyword_plus_weight = src_obj.get("keywordPlusWeight") if "keywordPlusWeight" in src_obj else None
    use_close_variant = src_obj.get("useCloseVariant") if "useCloseVariant" in src_obj else None
    if use_keyword_plus is None and keyword_plus_weight is None and use_close_variant is None:
        return True, "원본 검색옵션 값이 없어 기본값 유지"
    ok, msg = _update_adgroup_search_options(
        api_key, secret_key, cid, target_adgroup_id,
        use_keyword_plus=None if use_keyword_plus is None else _normalize_bool_for_api(use_keyword_plus, False),
        keyword_plus_weight=keyword_plus_weight,
        use_close_variant=None if use_close_variant is None else _normalize_bool_for_api(use_close_variant, False),
    )
    return ok, msg
def _copy_adgroup_media_settings(api_key: str, secret_key: str, cid: str, source_adgroup_id: str, target_adgroup_id: str):
    messages: List[str] = []
    ok_pm, msg_pm = _copy_target_payload_exact(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id, "PC_MOBILE_TARGET")
    if not ok_pm:
        try:
            res_get, src_obj = _fetch_adgroup_detail(api_key, secret_key, cid, source_adgroup_id)
            if res_get.status_code == 200 and isinstance(src_obj, dict):
                pc = bool(src_obj.get("pcDevice", True))
                mobile = bool(src_obj.get("mobileDevice", True))
                media_type = "ALL" if (pc and mobile) else ("PC" if pc else ("MOBILE" if mobile else "ALL"))
                ok_pm, msg_pm = _update_pc_mobile_target(api_key, secret_key, cid, target_adgroup_id, media_type)
        except Exception:
            pass
    if msg_pm:
        messages.append(msg_pm)
    ok_media, msg_media = _copy_target_payload_exact(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id, "MEDIA_TARGET")
    if msg_media:
        messages.append(msg_media)
    return ok_pm and ok_media, messages
def _prepare_adgroup_full_update_obj_for_search_options(obj: Dict[str, Any], cid: str, target_values: Dict[str, Any], sanitized: bool = True) -> Dict[str, Any]:
    update_obj = dict(obj or {})
    update_obj.update(target_values or {})
    update_obj.setdefault("customerId", _safe_int_default(cid, 0))
    adgroup_id = str(update_obj.get("nccAdgroupId") or update_obj.get("id") or "").strip()
    if adgroup_id:
        update_obj["nccAdgroupId"] = adgroup_id

    # 전체 update(no fields)는 조회 응답의 읽기전용/요약성 필드를 그대로 보내면 실패할 수 있어 제거한다.
    # PC/MO 입찰가중치 패치에서 성공한 방식과 동일하게, 수정 대상 값은 유지하고 서버 계산값만 걷어낸다.
    if sanitized:
        readonly_keys = {
            "id", "raw", "targets", "targetSummary", "pcChannelKey", "mobileChannelKey",
            "status", "statusReason", "expectCost", "regTm", "editTm", "migType",
            "sharedDailyBudget", "sharedBudgetName", "sharedBudgetLock", "sharedBudgetExpectCost",
            "numberInUse", "pcDevice", "mobileDevice",
        }
        for key in readonly_keys:
            update_obj.pop(key, None)
    return {k: v for k, v in update_obj.items() if v is not None}


def _normalize_bool_for_api(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "y", "yes", "on", "사용", "예", "네"}


def _coerce_search_option_value(key: str, value: Any) -> Any:
    if key in {"useKeywordPlus", "useCloseVariant"}:
        return _normalize_bool_for_api(value, False)
    if key == "keywordPlusWeight":
        try:
            return int(float(value))
        except Exception:
            return value
    return value


def _same_search_option_value(key: str, actual: Any, expected: Any) -> bool:
    if key in {"useKeywordPlus", "useCloseVariant"}:
        return _normalize_bool_for_api(actual, False) == _normalize_bool_for_api(expected, False)
    if key == "keywordPlusWeight":
        try:
            return int(float(actual)) == int(float(expected))
        except Exception:
            return actual == expected
    return actual == expected


def _verify_adgroup_search_options(api_key: str, secret_key: str, cid: str, adgroup_id: str, expected: Dict[str, Any]) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    res, verify_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
    if res is not None and res.status_code != 200:
        return False, f"재조회 실패: {res.text}", None
    if not isinstance(verify_obj, dict):
        return False, "재조회 응답이 비어 있어 반영 여부를 확인하지 못했습니다.", None
    mismatches: List[str] = []
    missing: List[str] = []
    for key, expected_value in expected.items():
        if key not in verify_obj:
            missing.append(key)
            continue
        actual_value = verify_obj.get(key)
        if not _same_search_option_value(key, actual_value, expected_value):
            mismatches.append(f"{key}: 요청 {expected_value} / 확인 {actual_value}")
    if missing:
        return False, "재조회 응답에 확인 필드 없음: " + ", ".join(missing[:3]), verify_obj
    if mismatches:
        return False, "반영값 불일치: " + " / ".join(mismatches[:3]), verify_obj
    return True, "반영값 확인 완료", verify_obj


def _put_adgroup_by_fields(api_key: str, secret_key: str, cid: str, adgroup_id: str, values: Dict[str, Any], base_obj: Optional[Dict[str, Any]] = None, mode: str = "minimal"):
    clean_values = {k: _coerce_search_option_value(k, v) for k, v in (values or {}).items() if v is not None}
    if not clean_values:
        return None
    fields = ",".join(clean_values.keys())
    if mode == "full":
        payload = _prepare_adgroup_full_update_obj_for_search_options(base_obj or {}, cid, clean_values, sanitized=True)
    else:
        payload = dict(clean_values)
    return _do_req(
        "PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}",
        params={"fields": fields}, json_body=payload
    )


def _put_adgroup_by_fields_body_fallback(api_key: str, secret_key: str, cid: str, adgroup_id: str, values: Dict[str, Any], base_obj: Optional[Dict[str, Any]] = None):
    # 일부 레거시 예시에서는 fields를 body에 포함한 형태가 섞여 있어, 마지막 호환 fallback으로만 사용한다.
    clean_values = {k: _coerce_search_option_value(k, v) for k, v in (values or {}).items() if v is not None}
    if not clean_values:
        return None
    payload = _prepare_adgroup_full_update_obj_for_search_options(base_obj or {}, cid, clean_values, sanitized=True)
    payload["fields"] = ",".join(clean_values.keys())
    return _do_req(
        "PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}",
        params=None, json_body=payload
    )


def _search_option_needs_change(obj: Dict[str, Any], key: str, target_value: Any) -> bool:
    if key not in obj:
        return True
    return not _same_search_option_value(key, obj.get(key), target_value)


def _expanded_search_expected_from_inputs(use_keyword_plus: Optional[bool], use_close_variant: Optional[bool]) -> Optional[bool]:
    """Return requested current Naver PowerLink search mode.

    화면의 "확장검색" 선택값을 기준으로 신규 확장검색 ON/OFF를 결정한다.
    "일치검색"은 2025-01 이후 기본 노출 유형으로 동작하므로 별도 OFF 대상이 아니며,
    사용자가 "확장검색 미사용 + 일치검색 사용"을 선택하면 확장검색 OFF 요청으로 처리한다.
    """
    if use_keyword_plus is not None:
        return _normalize_bool_for_api(use_keyword_plus, False)
    if use_close_variant is not None and _normalize_bool_for_api(use_close_variant, False):
        return False
    return None


_EXPANDED_SEARCH_RESPONSE_KEYS = (
    # 실제 광고그룹 조회 응답에서 확인된 현재 필드.
    # 2026-04 테스트 로그 기준 응답 key: useExpSearch, expSearchBudgetRatio
    "useExpSearch",
    "expSearch",
    # 2024-10 이후 마스터 리포트의 "Using Expanded Search"에 대응될 가능성이 높은 후보.
    "useExpandedSearch",
    "usingExpandedSearch",
    "useExtendedSearch",
    "usingExtendedSearch",
    "expandedSearch",
    "expandedSearchUse",
    "expandedSearchFlag",
    "expandedSearchEnabled",
    "useExpandedSearchAd",
)

_EXPANDED_SEARCH_BUDGET_RATIO_KEYS = (
    "expandedSearchBudgetRatio",
    "expandedSearchBudgetRate",
    "expandedSearchBudgetPercent",
    "expSearchBudgetRatio",
)

_LEGACY_SEARCH_OPTION_KEYS = ("useKeywordPlus", "useCloseVariant", "keywordPlusWeight")


def _coerce_expanded_search_payload_value(key: str, enabled: bool, prefer_int: bool = False) -> Any:
    lk = str(key or "").lower()
    if prefer_int or lk.endswith("flag") or lk.startswith("using"):
        return 1 if enabled else 0
    return bool(enabled)


def _same_expanded_search_value(actual: Any, expected: bool) -> bool:
    if isinstance(actual, bool):
        return actual == bool(expected)
    s = str(actual).strip().lower()
    if s in {"1", "true", "y", "yes", "on", "사용", "used", "enable", "enabled"}:
        return bool(expected) is True
    if s in {"0", "false", "n", "no", "off", "미사용", "not_used", "disable", "disabled"}:
        return bool(expected) is False
    try:
        return int(float(actual)) == (1 if expected else 0)
    except Exception:
        return False


def _read_expanded_search_from_obj(obj: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[Any]]:
    if not isinstance(obj, dict):
        return None, None
    for key in _EXPANDED_SEARCH_RESPONSE_KEYS:
        if key in obj:
            return key, obj.get(key)
    # API 응답명이 문서보다 먼저 바뀌는 경우를 대비해 key 이름으로 탐지한다.
    for key, value in obj.items():
        lk = str(key).lower()
        # 실제 응답 key가 useExpSearch처럼 expanded가 아니라 exp 축약형으로 내려오는 경우가 있다.
        if (("expanded" in lk or "exp" in lk) and "search" in lk and not lk.endswith("ratio") and "budget" not in lk):
            return str(key), value
    return None, None


def _read_expanded_budget_ratio_from_obj(obj: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[Any]]:
    if not isinstance(obj, dict):
        return None, None
    for key in _EXPANDED_SEARCH_BUDGET_RATIO_KEYS:
        if key in obj:
            return key, obj.get(key)
    for key, value in obj.items():
        lk = str(key).lower()
        if "expanded" in lk and "search" in lk and ("budget" in lk or "ratio" in lk or "rate" in lk):
            return str(key), value
    return None, None


def _verify_adgroup_expanded_search(api_key: str, secret_key: str, cid: str, adgroup_id: str, expected_enabled: bool, response_obj: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    key, val = _read_expanded_search_from_obj(response_obj)
    if key is not None:
        if _same_expanded_search_value(val, expected_enabled):
            return True, f"응답값 확인 완료({key}={val})", response_obj
        return False, f"응답값 불일치({key}: 요청 {expected_enabled} / 응답 {val})", response_obj

    res, verify_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
    if res is not None and res.status_code != 200:
        return False, f"재조회 실패: {res.text}", None
    key, val = _read_expanded_search_from_obj(verify_obj)
    if key is None:
        keys_preview = ", ".join(list((verify_obj or {}).keys())[:35]) if isinstance(verify_obj, dict) else ""
        return False, "재조회 응답에 신규 확장검색 확인 필드가 없습니다." + (f" 확인된 key: {keys_preview}" if keys_preview else ""), verify_obj
    if not _same_expanded_search_value(val, expected_enabled):
        return False, f"반영값 불일치({key}: 요청 {expected_enabled} / 확인 {val})", verify_obj
    return True, f"반영값 확인 완료({key}={val})", verify_obj


def _verify_adgroup_legacy_search_options(api_key: str, secret_key: str, cid: str, adgroup_id: str, expected_values: Dict[str, Any], response_obj: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    # 구형 키워드 확장 Beta 필드가 응답에 실제 존재하는 계정/상품용 검증.
    obj = response_obj if isinstance(response_obj, dict) else None
    if obj is None:
        res, obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
        if res is not None and res.status_code != 200:
            return False, f"재조회 실패: {res.text}", None
    if not isinstance(obj, dict):
        return False, "재조회 응답이 비어 있어 legacy 검색옵션 반영 여부를 확인하지 못했습니다.", None
    missing: List[str] = []
    mismatches: List[str] = []
    for key, expected in (expected_values or {}).items():
        if key not in obj:
            missing.append(key)
            continue
        actual = obj.get(key)
        if not _same_search_option_value(key, actual, expected):
            mismatches.append(f"{key}: 요청 {expected} / 확인 {actual}")
    if missing:
        return False, "legacy 확인 필드 없음: " + ", ".join(missing[:3]), obj
    if mismatches:
        return False, "legacy 반영값 불일치: " + " / ".join(mismatches[:3]), obj
    return True, "legacy 반영값 확인 완료", obj


def _sanitize_adgroup_for_expanded_search_update(obj: Dict[str, Any], cid: str, update_values: Dict[str, Any]) -> Dict[str, Any]:
    payload = _prepare_adgroup_full_update_obj_for_search_options(obj or {}, cid, update_values, sanitized=True)
    for key in [
        "targetSummary", "targets", "raw", "status", "statusReason", "expectCost",
        "sharedDailyBudget", "sharedBudgetName", "sharedBudgetLock", "sharedBudgetExpectCost",
    ]:
        payload.pop(key, None)
    return {k: v for k, v in payload.items() if v is not None}


def _put_adgroup_expanded_search_candidate(api_key: str, secret_key: str, cid: str, adgroup_id: str, base_obj: Dict[str, Any], field_param: Optional[str], payload_values: Dict[str, Any], full_payload: bool = False):
    if full_payload:
        payload = _sanitize_adgroup_for_expanded_search_update(base_obj or {}, cid, payload_values)
    else:
        payload = dict(payload_values)
    params = {"fields": field_param} if field_param else None
    return _do_req("PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}", params=params, json_body=payload)


def _normalize_expanded_budget_ratio(value: Any) -> Optional[int]:
    if str(value or "").strip() == "":
        return None
    try:
        n = int(float(value))
    except Exception:
        return None
    return max(1, min(100, n))


def _expanded_search_candidates(obj: Dict[str, Any], expected_enabled: bool, keyword_plus_weight: Optional[int] = None, use_close_variant: Optional[bool] = None) -> List[Dict[str, Any]]:
    detected_key, detected_val = _read_expanded_search_from_obj(obj)
    detected_budget_key, detected_budget_val = _read_expanded_budget_ratio_from_obj(obj)
    budget_ratio = _normalize_expanded_budget_ratio(keyword_plus_weight)
    candidates: List[Dict[str, Any]] = []
    seen = set()

    def add(label: str, field_param: Optional[str], values: Dict[str, Any], full_payload: bool = False, verify_kind: str = "expanded", verify_values: Optional[Dict[str, Any]] = None):
        clean = {k: v for k, v in (values or {}).items() if v is not None}
        if not clean:
            return
        sig = (label, field_param or "", json.dumps(clean, ensure_ascii=False, sort_keys=True), bool(full_payload), verify_kind)
        if sig in seen:
            return
        seen.add(sig)
        candidates.append({
            "label": label,
            "field_param": field_param,
            "values": clean,
            "full_payload": bool(full_payload),
            "verify_kind": verify_kind,
            "verify_values": verify_values or clean,
        })

    def maybe_with_budget(values: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(values or {})
        if budget_ratio is not None:
            if detected_budget_key:
                out[detected_budget_key] = budget_ratio
            else:
                out["expandedSearchBudgetRatio"] = budget_ratio
        return out

    # 1) PC/MO 입찰가중치처럼 전체 광고그룹 update(no fields)를 먼저 시도한다.
    #    3726은 fields 부분수정에서 주로 발생하므로, fields 없는 전체 update가 가장 안전한 우선 전략이다.
    if detected_key:
        prefer_int = isinstance(detected_val, int) and not isinstance(detected_val, bool)
        value = _coerce_expanded_search_payload_value(detected_key, expected_enabled, prefer_int=prefer_int)
        add(f"full/no-fields/detected-{detected_key}", None, maybe_with_budget({detected_key: value}), True, "expanded")

    current_key_candidates = [
        # 실제 조회 응답에서 확인된 현재 API 필드 우선
        ("useExpSearch", bool(expected_enabled)),
        ("expSearch", bool(expected_enabled)),
        ("useExpandedSearch", bool(expected_enabled)),
        ("usingExpandedSearch", 1 if expected_enabled else 0),
        ("useExtendedSearch", bool(expected_enabled)),
        ("usingExtendedSearch", 1 if expected_enabled else 0),
        ("expandedSearch", bool(expected_enabled)),
        ("expandedSearchUse", 1 if expected_enabled else 0),
        ("expandedSearchFlag", 1 if expected_enabled else 0),
        ("expandedSearchEnabled", bool(expected_enabled)),
    ]
    for key, value in current_key_candidates:
        add(f"full/no-fields/{key}", None, maybe_with_budget({key: value}), True, "expanded")

    # 2) 구형 필드가 아직 응답에 존재하는 계정 대비: 전체 update(no fields)로 먼저 시도한다.
    #    fields=useKeywordPlus/useCloseVariant는 현재 3726이 날 수 있으므로 fallback 맨 뒤에 둔다.
    legacy_values = {
        "useKeywordPlus": bool(expected_enabled),
        "useCloseVariant": True if use_close_variant is None else _normalize_bool_for_api(use_close_variant, True),
        "keywordPlusWeight": budget_ratio if expected_enabled and budget_ratio is not None else (100 if expected_enabled else 0),
    }
    if any(k in (obj or {}) for k in _LEGACY_SEARCH_OPTION_KEYS):
        add("full/no-fields/legacy-useKeywordPlus-useCloseVariant", None, legacy_values, True, "legacy", legacy_values)
    else:
        # 응답에 필드가 없어도 일부 계정에서 전체 update로만 받아주는 경우가 있어 한 번은 시도한다.
        add("full/no-fields/legacy-compat", None, legacy_values, True, "legacy", legacy_values)

    # 3) fields 부분수정은 뒤쪽 fallback으로만 사용한다. 3726이 나도 전체 그룹 반복 실패를 피하기 위해 메시지를 축약한다.
    if detected_key:
        prefer_int = isinstance(detected_val, int) and not isinstance(detected_val, bool)
        value = _coerce_expanded_search_payload_value(detected_key, expected_enabled, prefer_int=prefer_int)
        add(f"fields/detected-{detected_key}", detected_key, {detected_key: value}, False, "expanded")
    for key, value in current_key_candidates:
        add(f"fields/{key}", key, {key: value}, False, "expanded")

    # legacy fields fallback은 정말 마지막.
    add("fields/legacy-useKeywordPlus", "useKeywordPlus,keywordPlusWeight,useCloseVariant", legacy_values, False, "legacy", legacy_values)
    return candidates


def _compact_api_error(text: str, max_len: int = 180) -> str:
    t = str(text or "").strip()
    if len(t) > max_len:
        return t[:max_len] + "..."
    return t


def _update_adgroup_search_options(api_key: str, secret_key: str, cid: str, adgroup_id: str, use_keyword_plus: Optional[bool] = None, keyword_plus_weight: Optional[int] = None, use_close_variant: Optional[bool] = None):
    expected_expanded = _expanded_search_expected_from_inputs(use_keyword_plus, use_close_variant)
    if expected_expanded is None:
        return True, f"변경사항 없음 · patch={PATCH_VERSION}"

    res_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}")
    if res_get.status_code != 200:
        return False, f"광고그룹 조회 실패: {res_get.text} · patch={PATCH_VERSION}"
    obj = res_get.json() or {}
    if str(obj.get("adgroupType") or "").upper() != "WEB_SITE":
        return True, f"파워링크 그룹이 아니어서 검색옵션 변경 건너뜀 · patch={PATCH_VERSION}"

    current_key, current_value = _read_expanded_search_from_obj(obj)
    if current_key is not None and _same_expanded_search_value(current_value, expected_expanded):
        return True, f"이미 요청값과 동일({current_key}={current_value}) · patch={PATCH_VERSION}"

    failed_details: List[str] = []
    last_verify_obj: Optional[Dict[str, Any]] = obj
    not_support_seen = 0
    for cand in _expanded_search_candidates(obj, expected_expanded, keyword_plus_weight=keyword_plus_weight, use_close_variant=use_close_variant):
        label = str(cand.get("label") or "candidate")
        field_param = cand.get("field_param")
        values = cand.get("values") or {}
        full_payload = bool(cand.get("full_payload"))
        verify_kind = str(cand.get("verify_kind") or "expanded")
        try:
            res = _put_adgroup_expanded_search_candidate(api_key, secret_key, cid, adgroup_id, last_verify_obj or obj, field_param, values, full_payload=full_payload)
        except Exception as e:
            failed_details.append(f"{label}: 요청 예외 {e}")
            continue
        response_text = res.text if res is not None else "응답 없음"
        response_obj = None
        try:
            response_obj = res.json() if res is not None and response_text else None
        except Exception:
            response_obj = None

        if res is not None and res.status_code in [200, 201, 204]:
            if verify_kind == "legacy":
                ok_verify, verify_msg, verify_obj = _verify_adgroup_legacy_search_options(api_key, secret_key, cid, adgroup_id, cand.get("verify_values") or values, response_obj if isinstance(response_obj, dict) else None)
            else:
                ok_verify, verify_msg, verify_obj = _verify_adgroup_expanded_search(api_key, secret_key, cid, adgroup_id, expected_expanded, response_obj if isinstance(response_obj, dict) else None)
            if isinstance(verify_obj, dict):
                last_verify_obj = verify_obj
            if ok_verify:
                msg = f"확장검색 {'사용' if expected_expanded else '미사용'} 적용 완료 ({label}, {verify_msg}) · patch={PATCH_VERSION}"
                if use_close_variant is not None:
                    msg += " · 일치검색은 현재 네이버 기본 동작으로 유지됩니다."
                return True, msg
            failed_details.append(f"{label}: 응답 성공이나 {verify_msg}")
            continue

        short_text = _compact_api_error(response_text)
        if '"code":3726' in short_text.replace(" ", "") or "Not support modify field" in short_text:
            not_support_seen += 1
            # 같은 3726이 길게 반복되지 않게 label만 남긴다.
            failed_details.append(f"{label}: 3726 Not support modify field")
        else:
            failed_details.append(f"{label}: {short_text}")

    checked_keys = ", ".join(list((last_verify_obj or obj or {}).keys())[:35]) if isinstance((last_verify_obj or obj), dict) else ""
    prefix = "광고그룹 확장검색/일치검색 변경 실패 또는 미반영"
    if not_support_seen:
        prefix += f" (fields 방식 3726 {not_support_seen}회 감지, no-fields 전체 update도 함께 시도함)"
    detail = " / ".join(failed_details[:10])
    if checked_keys:
        detail += f" / 확인된 광고그룹 응답 key: {checked_keys}"
    return False, f"{prefix}: {detail} · patch={PATCH_VERSION}"

def _extract_restricted_rows_from_target(target_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(target_obj, dict):
        return rows
    for item in target_obj.get("target") or []:
        if not isinstance(item, dict):
            continue
        kw = str(item.get("keyword") or "").strip()
        if not kw:
            continue
        rows.append({
            "keyword": kw,
            "type": item.get("type"),
            "matchType": _label_negative_type(item.get("type")),
            "ownerId": target_obj.get("ownerId"),
            "nccTargetId": target_obj.get("nccTargetId"),
        })
    return rows
def _fetch_restricted_keywords(api_key: str, secret_key: str, cid: str, adgroup_id: str):
    detail_res, adgroup_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
    is_shopping = _is_shopping_adgroup(adgroup_obj)
    if is_shopping:
        res_target, target_obj = _fetch_target_restrict_object(api_key, secret_key, cid, adgroup_id)
        if res_target.status_code == 200:
            rows = _extract_restricted_rows_from_target(target_obj) if target_obj else []
            rows.sort(key=lambda x: (str(x.get("keyword") or "").lower(), str(x.get("matchType") or "")))
            return res_target, rows
        return res_target, []
    rows: List[Dict[str, Any]] = []
    res = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}/restricted-keywords", params={"type": "KEYWORD_PLUS_RESTRICT"})
    if res.status_code == 200:
        for item in res.json() or []:
            if isinstance(item, dict):
                kw = str(item.get("keyword") or item.get("restrictedKeyword") or "").strip()
                if kw:
                    rows.append({
                        "keyword": kw,
                        "type": item.get("type") or "KEYWORD_PLUS_RESTRICT",
                        "matchType": _label_negative_type(item.get("type") or "KEYWORD_PLUS_RESTRICT"),
                        "ownerId": item.get("ownerId") or item.get("nccAdgroupId") or adgroup_id,
                    })
            elif isinstance(item, str) and item.strip():
                rows.append({"keyword": item.strip(), "type": "KEYWORD_PLUS_RESTRICT", "matchType": "일치", "ownerId": adgroup_id})
        rows.sort(key=lambda x: (str(x.get("keyword") or "").lower(), str(x.get("matchType") or "")))
        return res, rows
    if detail_res.status_code == 200:
        return _make_fake_response(200, "[]"), []
    return res, []
def _normalize_ext_compare_type(value: Any) -> str:
    s = str(value or "").strip().upper()
    if s == "SHOPPING_PROMO_TEXT":
        return "PROMOTION"
    if s == "쇼핑상품부가정보".upper():
        return "SHOPPING_EXTRA"
    return s
def _extension_matches(ext_item: Dict[str, Any], ext_type: str, data: Dict[str, Any]) -> bool:
    if not isinstance(ext_item, dict):
        return False
    item_type = _normalize_ext_compare_type(ext_item.get("type"))
    target_type = _normalize_ext_compare_type(ext_type)
    if item_type != target_type:
        return False
    ext = ext_item.get("adExtension")
    if target_type == "HEADLINE":
        return str((ext or {}).get("headline") or "").strip() == str(data.get("headline") or "").strip()
    if target_type in {"DESCRIPTION_EXTRA", "DESCRIPTION"}:
        return str((ext or {}).get("description") or "").strip() == str(data.get("description") or "").strip()
    if target_type == "PROMOTION":
        ext = ext or {}
        return (
            str(ext.get("basicText") or "").strip() == str(data.get("basicText") or "").strip()
            and str(ext.get("additionalText") or "").strip() == str(data.get("additionalText") or "").strip()
        )
    if target_type == "SUB_LINKS":
        if not isinstance(ext, list):
            return False
        left = sorted(
            (str(x.get("name") or "").strip(), str(x.get("final") or "").strip())
            for x in ext if isinstance(x, dict)
        )
        right = sorted(
            (str(x.get("name") or "").strip(), str(x.get("final") or "").strip())
            for x in (data.get("links") or []) if isinstance(x, dict)
        )
        return left == right
    if target_type == "SHOPPING_EXTRA":
        return True
    return False
def _find_existing_extension(api_key: str, secret_key: str, cid: str, owner_id: str, ext_type: str, data: Dict[str, Any]):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": owner_id})
    if res.status_code != 200:
        return None
    try:
        items = res.json() or []
    except Exception:
        return None
    for item in items:
        if _extension_matches(item, ext_type, data):
            return item
    return None
def _build_extension_payload(owner_id: str, ext_type: str, data: Dict[str, Any], customer_id: int, position: int | None = None) -> Dict[str, Any]:
    ext_type = _normalize_extension_type(ext_type)
    payload: Dict[str, Any] = {"ownerId": owner_id, "customerId": customer_id, "type": ext_type}
    if ext_type == "HEADLINE":
        ad_ext = {"headline": str(data.get("headline") or "").strip()}
        if position in {1, 2}:
            # 공식 이슈 답변 기준: HEADLINE 노출 위치는 top-level priority/displayOrder가 아니라
            # adExtension.pin 값(1 또는 2)으로 지정합니다.
            ad_ext["pin"] = int(position)
        payload["adExtension"] = ad_ext
    elif ext_type in {"DESCRIPTION_EXTRA", "DESCRIPTION"}:
        payload["adExtension"] = {"description": str(data.get("description") or "").strip()}
    elif ext_type == "PROMOTION":
        promo = {"basicText": str(data.get("basicText") or "").strip()}
        additional = str(data.get("additionalText") or "").strip()
        if additional:
            promo["additionalText"] = additional
        payload["adExtension"] = promo
    elif ext_type == "SUB_LINKS":
        payload["adExtension"] = [
            {"name": str(x.get("name") or "").strip(), "final": str(x.get("final") or "").strip()}
            for x in (data.get("links") or []) if str(x.get("name") or "").strip() and str(x.get("final") or "").strip()
        ]
    elif ext_type == "SHOPPING_EXTRA":
        # 쇼핑상품부가정보는 소재(ownerId=nccAdId)에 매핑되며 adExtension 본문 없이 타입만 요청하는 방식이 가장 호환성이 높습니다.
        pass
    return payload
def _extract_extension_item(obj: Any) -> Dict[str, Any] | None:
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                return item
    return None
def _extract_headline_pin(ext_item: Dict[str, Any] | None) -> int | None:
    if not isinstance(ext_item, dict):
        return None
    ext = ext_item.get('adExtension')
    candidates: List[Any] = []
    if isinstance(ext, dict):
        candidates.extend([ext.get('pin'), ext.get('Pin'), ext.get('position'), ext.get('priority'), ext.get('displayOrder')])
    candidates.extend([ext_item.get('pin'), ext_item.get('Pin'), ext_item.get('position'), ext_item.get('priority'), ext_item.get('displayOrder')])
    for value in candidates:
        try:
            pin = int(str(value).strip())
        except Exception:
            continue
        if pin in {1, 2}:
            return pin
    return None
def _headline_pin_label(value: int | None) -> str:
    return {1: '위치1', 2: '위치2'}.get(value, '모든 위치')
def _verify_headline_pin(api_key: str, secret_key: str, cid: str, owner_id: str, headline: str, expected_position: int | None):
    existing = _find_existing_extension(api_key, secret_key, cid, owner_id, 'HEADLINE', {'headline': headline})
    if not existing:
        return {
            'ok': False,
            'item': None,
            'expected_pin': expected_position,
            'actual_pin': None,
            'detail': '등록 후 재조회에서 추가제목을 찾지 못했습니다.'
        }
    actual_pin = _extract_headline_pin(existing)
    ok = actual_pin == expected_position
    if ok:
        detail = f"재조회 검증 완료 ({_headline_pin_label(actual_pin)})"
    else:
        detail = f"재조회 검증 불일치 (요청: {_headline_pin_label(expected_position)} / 실제: {_headline_pin_label(actual_pin)})"
    return {
        'ok': ok,
        'item': existing,
        'expected_pin': expected_position,
        'actual_pin': actual_pin,
        'detail': detail,
    }
def _build_headline_pin_update_candidates(ext_item: Dict[str, Any], position: int | None, customer_id: int) -> List[Dict[str, Any]]:
    owner_id = str(ext_item.get("ownerId") or "").strip()
    ext_id = _extract_ad_extension_id(ext_item)
    headline = str(((ext_item.get("adExtension") or {}) if isinstance(ext_item.get("adExtension"), dict) else {}).get("headline") or "").strip()
    if not owner_id or not ext_id or not headline:
        return []
    ad_ext: Dict[str, Any] = {"headline": headline}
    if position in {1, 2}:
        ad_ext["pin"] = int(position)
    common = {
        "ownerId": owner_id,
        "customerId": int(customer_id),
        "type": "HEADLINE",
        "adExtension": ad_ext,
    }
    for k in ("pcChannelId", "mobileChannelId", "schedule", "usePeriod", "userLock", "status"):
        if k in ext_item and ext_item.get(k) is not None:
            common[k] = ext_item.get(k)
    candidates: List[Dict[str, Any]] = []
    seen = set()
    def add(payload: Dict[str, Any]):
        key = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if key not in seen:
            seen.add(key)
            candidates.append(payload)
    add(common)
    add({
        "ownerId": owner_id,
        "customerId": int(customer_id),
        "type": "HEADLINE",
        "adExtension": ad_ext,
    })
    if position in {1, 2}:
        add({**common, "priority": int(position)})
        add({**common, "displayOrder": int(position)})
        add({**common, "priority": int(position), "displayOrder": int(position)})
        add({**common, "adExtension": {"headline": headline, "headlinePin": position == 1}})
        add({**common, "priority": int(position), "adExtension": {"headline": headline, "headlinePin": position == 1}})
        add({**common, "displayOrder": int(position), "adExtension": {"headline": headline, "headlinePin": position == 1}})
    return candidates
def _apply_headline_pin_best_effort(api_key: str, secret_key: str, cid: str, ext_item: Dict[str, Any] | None, position: int | None):
    if position not in {None, 1, 2} or not isinstance(ext_item, dict):
        return {"ok": False, "detail": "추가제목 위치 보정 대상이 아닙니다."}
    ext_id = _extract_ad_extension_id(ext_item)
    if not ext_id:
        return {"ok": False, "detail": "확장소재 ID를 찾지 못했습니다."}
    tried: List[Dict[str, Any]] = []
    for payload in _build_headline_pin_update_candidates(ext_item, position, int(cid)):
        res = _do_req("PUT", api_key, secret_key, cid, f"/ncc/ad-extensions/{ext_id}", json_body=payload)
        tried.append({"status": res.status_code, "detail": res.text, "payload": payload})
        if res.status_code in [200, 201, 204]:
            item = ext_item
            try:
                parsed = res.json()
                item = _extract_extension_item(parsed) or item
            except Exception:
                pass
            label = _headline_pin_label(position)
            return {"ok": True, "item": item, "detail": f"{label} 반영 성공", "tried": tried}
    last = tried[-1] if tried else {}
    label = _headline_pin_label(position)
    return {"ok": False, "detail": last.get("detail") or f"{label} 반영 실패", "tried": tried}
def _apply_headline_position_best_effort(api_key: str, secret_key: str, cid: str, ext_item: Dict[str, Any] | None, position: int | None):
    return _apply_headline_pin_best_effort(api_key, secret_key, cid, ext_item, position)
def _shopping_extra_payload_candidates(owner_id: str, customer_id: int) -> List[Dict[str, Any]]:
    base = {"ownerId": owner_id, "customerId": int(customer_id), "type": "SHOPPING_EXTRA"}
    candidates: List[Dict[str, Any]] = [
        dict(base),
        {**base, "adExtension": {"showReviewCount": True, "showReviewRating": True, "showWishCount": True, "showPurchaseCount": True}},
        {**base, "adExtension": {"useReviewCount": True, "useReviewRating": True, "useWishCount": True, "usePurchaseCount": True}},
        {**base, "adExtension": {"reviewCount": True, "reviewRating": True, "wishCount": True, "purchaseCount": True}},
        {**base, "adExtension": {"reviewCnt": True, "reviewScore": True, "wishCnt": True, "purchaseCnt": True}},
        {**base, "adExtension": {"itemTypes": ["REVIEW_COUNT", "REVIEW_RATING", "WISH_COUNT", "PURCHASE_COUNT"]}},
        {**base, "adExtension": {"items": ["REVIEW_COUNT", "REVIEW_RATING", "WISH_COUNT", "PURCHASE_COUNT"]}},
        {**base, "adExtension": {"enabled": True}},
        {**base, "adExtension": {}},
    ]
    uniq: List[Dict[str, Any]] = []
    seen = set()
    for payload in candidates:
        key = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(payload)
    return uniq
def _create_shopping_extra_with_fallbacks(api_key: str, secret_key: str, cid: str, owner_id: str):
    attempts: List[Dict[str, Any]] = []
    last_res = None
    for idx, payload in enumerate(_shopping_extra_payload_candidates(owner_id, int(cid)), start=1):
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
        last_res = res
        if res.status_code in [200, 201]:
            return True, res, payload, attempts
        attempts.append({
            "try": idx,
            "status": res.status_code,
            "detail": (res.text or "")[:500],
            "payload": payload,
        })
        if res.status_code == 400 and "A record with the same name already exists." in (res.text or ""):
            return False, res, payload, attempts
    return False, last_res, (attempts[-1]["payload"] if attempts else None), attempts
def _bulk_create_campaigns(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "campaign", cid)
        name = str(payload.get("name") or f"{idx}행")
        if not payload.get("name") or not payload.get("campaignTp"):
            fail += 1
            results.append(_result_item(idx, False, name, "name / campaignTp는 필수입니다."))
            continue
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/campaigns", json_body=payload)
        if res.status_code in [200, 201]:
            success += 1
            results.append(_result_item(idx, True, name, "생성 완료"))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, res.text))
    return success, fail, results
def _bulk_create_adgroups(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "adgroup", cid)
        name = str(payload.get("name") or f"{idx}행")
        if not payload.get("nccCampaignId") or not payload.get("name") or not payload.get("adgroupType"):
            fail += 1
            results.append(_result_item(idx, False, name, "nccCampaignId / name / adgroupType는 필수입니다."))
            continue
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/adgroups", json_body=payload)
        if res.status_code in [200, 201]:
            success += 1
            results.append(_result_item(idx, True, name, "생성 완료"))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, res.text))
    return success, fail, results
def _bulk_create_keywords(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    grouped: Dict[str, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "keyword", cid)
        adg_id = str(payload.get("nccAdgroupId") or "").strip()
        name = str(payload.get("keyword") or f"{idx}행")
        if not adg_id or not payload.get("keyword"):
            fail += 1
            results.append(_result_item(idx, False, name, "nccAdgroupId / keyword는 필수입니다."))
            continue
        grouped[adg_id].append((idx, payload))
    batch_size = 30
    for adg_id, items in grouped.items():
        for start_idx in range(0, len(items), batch_size):
            chunk = items[start_idx:start_idx + batch_size]
            payloads = [x[1] for x in chunk]
            res = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id}, json_body=payloads)
            if res.status_code in [200, 201]:
                for idx, payload in chunk:
                    success += 1
                    results.append(_result_item(idx, True, str(payload.get("keyword")), "생성 완료"))
                continue
            with ThreadPoolExecutor(max_workers=min(8, len(chunk))) as ex:
                future_map = {
                    ex.submit(_do_req, "POST", api_key, secret_key, cid, "/ncc/keywords", {"nccAdgroupId": adg_id}, payload): (idx, payload)
                    for idx, payload in chunk
                }
                for fut in as_completed(future_map):
                    idx, payload = future_map[fut]
                    try:
                        single = fut.result()
                    except Exception as e:
                        fail += 1
                        results.append(_result_item(idx, False, str(payload.get("keyword")), str(e)))
                        continue
                    if single.status_code in [200, 201]:
                        success += 1
                        results.append(_result_item(idx, True, str(payload.get("keyword")), "생성 완료"))
                    else:
                        fail += 1
                        results.append(_result_item(idx, False, str(payload.get("keyword")), single.text))
    return success, fail, sorted(results, key=lambda x: x["row_no"])
def _post_one_ad(api_key: str, secret_key: str, cid: str, row_no: int, payload: Dict[str, Any]):
    adg_id = str(payload.get("nccAdgroupId") or "").strip()
    ad_type = str(payload.get("type") or "")
    name = str((payload.get("ad") or {}).get("headline") if isinstance(payload.get("ad"), dict) else payload.get("type") or f"{row_no}행")
    if not adg_id or not ad_type:
        return _result_item(row_no, False, name, "nccAdgroupId / type는 필수입니다.")
    body = copy.deepcopy(payload)
    if ad_type == "TEXT_45":
        ad = body.get("ad") or {}
        if not isinstance(ad, dict):
            return _result_item(row_no, False, name, "TEXT_45 소재는 ad JSON 객체가 필요합니다.")
        if isinstance(ad.get("pc"), dict) and ad["pc"].get("final") and not ad["pc"].get("display"):
            ad["pc"]["display"] = _display_url_from_final(ad["pc"]["final"])
        if isinstance(ad.get("mobile"), dict) and ad["mobile"].get("final") and not ad["mobile"].get("display"):
            ad["mobile"]["display"] = _display_url_from_final(ad["mobile"]["final"])
        missing = []
        if not ad.get("headline"):
            missing.append("headline")
        if not ad.get("description"):
            missing.append("description")
        if not (ad.get("pc") or {}).get("final"):
            missing.append("pc.final")
        if not (ad.get("mobile") or {}).get("final"):
            missing.append("mobile.final")
        if missing:
            return _result_item(row_no, False, name, f"TEXT_45 필수값 누락: {', '.join(missing)}")
        body["ad"] = ad
    if ad_type in SHOPPING_AD_TYPES:
        if "ad" not in body:
            body["ad"] = {}
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": adg_id, "isList": "true"}, json_body=[body])
    else:
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": adg_id}, json_body=body)
    if res.status_code in [200, 201]:
        return _result_item(row_no, True, name, "생성 완료")
    return _result_item(row_no, False, name, res.text)
def _bulk_create_ads(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    prepared = [(idx, _prepare_payload_row(row, "ad", cid)) for idx, row in enumerate(rows, start=1)]
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(_post_one_ad, api_key, secret_key, cid, idx, payload) for idx, payload in prepared]
        for future in as_completed(futures):
            results.append(future.result())
    success = sum(1 for x in results if x["ok"])
    fail = sum(1 for x in results if not x["ok"])
    return success, fail, sorted(results, key=lambda x: x["row_no"])
def _bulk_create_extensions(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "ad_extension", cid)
        owner_id = str(payload.get("ownerId") or "").strip()
        ext_type = str(payload.get("type") or "")
        name = str(payload.get("title") or ext_type or f"{idx}행")
        if not owner_id or not ext_type:
            fail += 1
            results.append(_result_item(idx, False, name, "ownerId / type는 필수입니다."))
            continue
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
        if res.status_code in [200, 201]:
            success += 1
            results.append(_result_item(idx, True, name, "생성 완료"))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, res.text))
    return success, fail, results
def _merge_restricted_keyword_rows(existing_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen = set()
    for item in (existing_rows or []):
        kw = str(item.get("keyword") or "").strip()
        if not kw:
            continue
        tp = _normalize_negative_type(item.get("type") or item.get("matchType") or 1)
        key = (kw.lower(), tp)
        if key not in seen:
            seen.add(key)
            merged.append({"keyword": kw, "type": tp})
    for item in (new_rows or []):
        kw = str(item.get("keyword") or "").strip()
        if not kw:
            continue
        tp = _normalize_negative_type(item.get("type") or item.get("matchType") or 1)
        key = (kw.lower(), tp)
        if key not in seen:
            seen.add(key)
            merged.append({"keyword": kw, "type": tp})
    return merged
def _upsert_shopping_restricted_keywords(api_key: str, secret_key: str, cid: str, adgroup_id: str, rows: List[Dict[str, Any]]):
    res_target, target_obj = _fetch_target_restrict_object(api_key, secret_key, cid, adgroup_id)
    if res_target.status_code != 200:
        return res_target
    if not target_obj or not target_obj.get("nccTargetId"):
        return _make_fake_response(400, "쇼핑 제외키워드 Target 정보를 찾지 못했습니다. 광고센터에서 해당 광고그룹의 제외키워드를 한 번 저장한 뒤 다시 시도해주세요.")
    merged = _merge_restricted_keyword_rows(_extract_restricted_rows_from_target(target_obj), rows)
    payload = {
        "nccTargetId": target_obj.get("nccTargetId"),
        "ownerId": adgroup_id,
        "targetTp": "RESTRICT_KEYWORD_TARGET",
        "target": merged,
        "delFlag": False,
    }
    return _do_req("PUT", api_key, secret_key, cid, f"/ncc/targets/{target_obj.get('nccTargetId')}", json_body=payload)
def _bulk_create_restricted_keywords(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    grouped: Dict[str, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "restricted_keyword", cid)
        adg_id = str(payload.get("nccAdgroupId") or "").strip()
        keyword = str(payload.get("keyword") or "").strip()
        if not adg_id or not keyword:
            fail += 1
            results.append(_result_item(idx, False, keyword or f"{idx}행", "nccAdgroupId / keyword는 필수입니다."))
            continue
        grouped[adg_id].append((idx, payload))
    for adg_id, items in grouped.items():
        _, adgroup_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adg_id)
        is_shopping = _is_shopping_adgroup(adgroup_obj)
        if is_shopping:
            res = _upsert_shopping_restricted_keywords(api_key, secret_key, cid, adg_id, [x[1] for x in items])
            if res.status_code in [200, 201, 204]:
                for idx, payload in items:
                    success += 1
                    results.append(_result_item(idx, True, str(payload.get("keyword")), "등록 완료"))
            else:
                for idx, payload in items:
                    fail += 1
                    results.append(_result_item(idx, False, str(payload.get("keyword")), res.text))
            continue
        payloads = []
        for _, payload in items:
            payloads.append({
                "nccAdgroupId": adg_id,
                "keyword": str(payload.get("keyword") or "").strip(),
                "description": "노출 제한 키워드 추가",
                "type": "KEYWORD_PLUS_RESTRICT",
            })
        res = _do_req("POST", api_key, secret_key, cid, f"/ncc/adgroups/{adg_id}/restricted-keywords", json_body=payloads)
        if res.status_code in [200, 201, 204]:
            for idx, payload in items:
                success += 1
                results.append(_result_item(idx, True, str(payload.get("keyword")), "등록 완료"))
        else:
            for idx, payload in items:
                single = _do_req("POST", api_key, secret_key, cid, f"/ncc/adgroups/{adg_id}/restricted-keywords", json_body=[{
                    "nccAdgroupId": adg_id,
                    "keyword": str(payload.get("keyword") or "").strip(),
                    "description": "노출 제한 키워드 추가",
                    "type": "KEYWORD_PLUS_RESTRICT",
                }])
                if single.status_code in [200, 201, 204]:
                    success += 1
                    results.append(_result_item(idx, True, str(payload.get("keyword")), "등록 완료"))
                else:
                    fail += 1
                    results.append(_result_item(idx, False, str(payload.get("keyword")), single.text))
    return success, fail, sorted(results, key=lambda x: x["row_no"])
def _fetch_entity_detail(api_key: str, secret_key: str, cid: str, entity_type: str, entity_id: str):
    entity_id = str(entity_id or "").strip()
    if not entity_id:
        return _make_fake_response(400, "ID가 없습니다."), None
    uri_map = {
        "ad": f"/ncc/ads/{entity_id}",
        "ad_extension": f"/ncc/ad-extensions/{entity_id}",
    }
    uri = uri_map.get(entity_type)
    if not uri:
        return _make_fake_response(400, f"지원하지 않는 조회 유형: {entity_type}"), None
    res = _do_req("GET", api_key, secret_key, cid, uri)
    if res.status_code == 200:
        try:
            obj = res.json()
            if isinstance(obj, list):
                for one in obj:
                    if isinstance(one, dict):
                        return res, one
            if isinstance(obj, dict):
                return res, obj
        except Exception:
            pass
    return res, None
def _build_copy_name(base_name: str, copy_index: int, total_count: int, suffix: str = "_복사본") -> str:
    base = str(base_name or "").strip()
    if total_count > 1:
        return f"{base}{copy_index}" if copy_index > 1 else base
    return base + str(suffix or "")
def _parse_multiline_names(raw_value: Any) -> List[str]:
    values: List[str] = []
    if isinstance(raw_value, list):
        iterable = raw_value
    else:
        iterable = str(raw_value or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    for item in iterable:
        name = str(item or "").strip()
        if name:
            values.append(name)
    return values
def _find_duplicate_names(names: List[str]) -> List[str]:
    seen: set[str] = set()
    dupes: List[str] = []
    for name in names:
        key = str(name or "").strip()
        if not key:
            continue
        if key in seen and key not in dupes:
            dupes.append(key)
            continue
        seen.add(key)
    return dupes
def _resolve_copy_names(source_names: List[str], copy_count: int, suffix: str, custom_names_raw: Any = None) -> List[str]:
    custom_names = _parse_multiline_names(custom_names_raw)
    total_needed = max(0, len(source_names)) * max(1, int(copy_count or 1))
    if custom_names:
        if len(custom_names) != total_needed:
            raise ValueError(f"직접 지정 이름은 총 {total_needed}개가 필요합니다. 현재 {len(custom_names)}개 입력됨")
        return custom_names
    resolved: List[str] = []
    for source_name in source_names:
        for idx in range(1, max(1, int(copy_count or 1)) + 1):
            resolved.append(_build_copy_name(source_name, idx, max(1, int(copy_count or 1)), suffix))
    return resolved
def _validate_name_candidates(candidates: List[str], existing_names: Iterable[str], label: str) -> Optional[str]:
    cleaned = [str(x or "").strip() for x in candidates if str(x or "").strip()]
    dupes = _find_duplicate_names(cleaned)
    if dupes:
        return f"중복된 {label} 이름이 있습니다: {', '.join(dupes[:10])}"
    existing = {str(x or "").strip() for x in existing_names if str(x or "").strip()}
    conflicts = [name for name in cleaned if name in existing]
    if conflicts:
        uniq_conflicts: List[str] = []
        for name in conflicts:
            if name not in uniq_conflicts:
                uniq_conflicts.append(name)
        return f"이미 존재하는 {label} 이름과 겹칩니다: {', '.join(uniq_conflicts[:10])}"
    return None
def _normalize_text_ad_payload(body: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = copy.deepcopy(body) if isinstance(body, dict) else {}
    ad = payload.get("ad") if isinstance(payload.get("ad"), dict) else {}
    headline = str(ad.get("headline") or payload.get("headline") or "").strip()
    description = str(ad.get("description") or payload.get("description") or "").strip()
    pc = ad.get("pc") if isinstance(ad.get("pc"), dict) else {}
    mobile = ad.get("mobile") if isinstance(ad.get("mobile"), dict) else {}
    pc_final = str(pc.get("final") or payload.get("pcFinalUrl") or payload.get("finalUrl") or "").strip()
    mobile_final = str(mobile.get("final") or payload.get("mobileFinalUrl") or payload.get("mobileUrl") or pc_final).strip()
    payload["ad"] = {
        "headline": headline,
        "description": description,
        "pc": {
            "final": pc_final,
            "display": str(pc.get("display") or _display_url_from_final(pc_final)).strip(),
        },
        "mobile": {
            "final": mobile_final,
            "display": str(mobile.get("display") or _display_url_from_final(mobile_final)).strip(),
        },
    }
    return _strip_empty(payload)
def _make_ad_signature(ad_item: Dict[str, Any] | None) -> str:
    ad = ad_item if isinstance(ad_item, dict) else {}
    ad_type = _normalize_ad_type(ad.get("type") or ad.get("adType"))
    if ad_type == "TEXT_45":
        ad_body = ad.get("ad") if isinstance(ad.get("ad"), dict) else {}
        pc = ad_body.get("pc") if isinstance(ad_body.get("pc"), dict) else {}
        mobile = ad_body.get("mobile") if isinstance(ad_body.get("mobile"), dict) else {}
        return "|".join([
            ad_type,
            str(ad_body.get("headline") or "").strip(),
            str(ad_body.get("description") or "").strip(),
            str(pc.get("final") or ad.get("pcFinalUrl") or "").strip(),
            str(mobile.get("final") or ad.get("mobileFinalUrl") or pc.get("final") or "").strip(),
        ])
    if ad_type in SHOPPING_AD_TYPES:
        return "|".join([ad_type, str(ad.get("referenceKey") or _extract_reference_key_from_ad(ad) or "").strip()])
    try:
        return "|".join([ad_type, json.dumps(ad.get("ad") or ad, ensure_ascii=False, sort_keys=True)])
    except Exception:
        return "|".join([ad_type, str(ad)])
def _collect_ad_signatures(api_key: str, secret_key: str, cid: str, adgroup_id: str) -> set[str]:
    res_ads, ads = _fetch_ads(api_key, secret_key, cid, str(adgroup_id))
    if res_ads.status_code != 200:
        return set()
    return {_make_ad_signature(ad) for ad in (ads or []) if isinstance(ad, dict)}
def _retry_missing_ads(api_key: str, secret_key: str, cid: str, new_adg_id: str, payloads: List[Dict[str, Any]], existing_signatures: set[str] | None = None) -> Tuple[int, List[str]]:
    signatures = set(existing_signatures or set())
    success = 0
    errors: List[str] = []
    for payload in payloads or []:
        item = copy.deepcopy(payload) if isinstance(payload, dict) else {}
        if _normalize_ad_type(item.get("type")) == "TEXT_45":
            item = _normalize_text_ad_payload(item)
        sig = _make_ad_signature(item)
        if sig in signatures:
            continue
        ad_type = _normalize_ad_type(item.get("type"))
        if ad_type in SHOPPING_AD_TYPES:
            res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id, "isList": "true"}, json_body=[item])
        else:
            res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id}, json_body=item)
        if res.status_code in [200, 201]:
            success += 1
            signatures.add(sig)
        else:
            ad_name = str(((item.get("ad") or {}).get("headline") if isinstance(item.get("ad"), dict) else "") or item.get("type") or "")
            errors.append(f"소재 재시도 에러({ad_name}): {res.text}")
    return success, errors
def _build_copy_ad_payload(api_key: str, secret_key: str, cid: str, ad_item: Dict[str, Any], new_adg_id: str) -> Dict[str, Any]:
    ad_item = ad_item if isinstance(ad_item, dict) else {}
    ad_id = str(ad_item.get("nccAdId") or ad_item.get("id") or "").strip()
    detail = None
    if ad_id:
        _, detail = _fetch_entity_detail(api_key, secret_key, cid, "ad", ad_id)
    src = detail if isinstance(detail, dict) and detail else copy.deepcopy(ad_item)
    payload = _prepare_payload_row(src, "ad", cid)
    payload["nccAdgroupId"] = str(new_adg_id)
    payload["customerId"] = int(cid)
    ad_type = _normalize_ad_type(payload.get("type") or src.get("type"))
    payload["type"] = ad_type
    ref_data = src.get("referenceData") if isinstance(src.get("referenceData"), dict) else {}
    if ad_type in SHOPPING_AD_TYPES:
        payload["ad"] = payload.get("ad") if isinstance(payload.get("ad"), dict) else {}
        ref_key = (
            payload.get("referenceKey")
            or src.get("referenceKey")
            or ref_data.get("mallProductId")
            or ref_data.get("id")
        )
        if ref_key:
            payload["referenceKey"] = str(ref_key)
    else:
        payload.pop("referenceKey", None)
    payload.pop("nccAdId", None)
    payload.pop("id", None)
    payload.pop("adId", None)
    return _strip_empty(payload)
def _build_copy_extension_payload(api_key: str, secret_key: str, cid: str, ext_item: Dict[str, Any], new_owner_id: str, biz_channel_id: str | None = None) -> Dict[str, Any]:
    ext_item = ext_item if isinstance(ext_item, dict) else {}
    ext_id = _extract_ad_extension_id(ext_item)
    detail = None
    if ext_id:
        _, detail = _fetch_entity_detail(api_key, secret_key, cid, "ad_extension", ext_id)
    src = detail if isinstance(detail, dict) and detail else copy.deepcopy(ext_item)
    payload = _prepare_payload_row(src, "ad_extension", cid)
    payload["ownerId"] = str(new_owner_id)
    payload["customerId"] = int(cid)
    ext_type = _normalize_extension_type(payload.get("type") or src.get("type"))
    payload["type"] = ext_type
    if biz_channel_id and biz_channel_id not in ["keep", "undefined"] and ext_type in ["SUB_LINKS", "POWER_LINK_IMAGE", "WEBSITE_INFO", "IMAGE_SUB_LINKS"]:
        payload["pcChannelId"] = str(biz_channel_id)
        payload["mobileChannelId"] = str(biz_channel_id)
    payload.pop("adExtensionId", None)
    payload.pop("id", None)
    if ext_type == "SHOPPING_EXTRA":
        payload.pop("adExtension", None)
    return _strip_empty(payload)
def _extract_created_ad_id_from_response(res: Any) -> str:
    if res is None:
        return ""
    try:
        data = res.json()
    except Exception:
        return ""
    def _walk(obj: Any) -> str:
        if isinstance(obj, dict):
            for key in ("nccAdId", "adId", "id"):
                value = str(obj.get(key) or "").strip()
                if value and (value.startswith("nad-") or value.startswith("ad-") or value.startswith("ncc") or len(value) >= 10):
                    return value
            for value in obj.values():
                found = _walk(value)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = _walk(item)
                if found:
                    return found
        return ""
    return _walk(data)
def _extract_reference_key_from_ad(ad_item: Dict[str, Any] | None) -> str:
    ad_item = ad_item if isinstance(ad_item, dict) else {}
    ref_data = ad_item.get("referenceData") if isinstance(ad_item.get("referenceData"), dict) else {}
    candidates = [
        ad_item.get("referenceKey"),
        ref_data.get("mallProductId"),
        ref_data.get("id"),
        ref_data.get("parentId"),
        ref_data.get("productId"),
        ref_data.get("mallProductNo"),
        ref_data.get("channelProductNo"),
        ref_data.get("productNo"),
    ]
    for value in candidates:
        s = str(value or "").strip()
        if s:
            return s
    return ""
def _classify_copy_ad_strategy(ads: List[Dict[str, Any]] | None) -> str:
    ads = [x for x in (ads or []) if isinstance(x, dict)]
    if not ads:
        return "none"
    shopping = 0
    standard = 0
    for ad in ads:
        if _normalize_ad_type(ad.get("type") or ad.get("adType")) in SHOPPING_AD_TYPES:
            shopping += 1
        else:
            standard += 1
    if shopping and standard:
        return "mixed"
    if shopping:
        return "shopping"
    return "standard"
def _find_created_ad_id_by_reference(api_key: str, secret_key: str, cid: str, adgroup_id: str, reference_key: str, before_ids: set[str] | None = None) -> str:
    reference_key = str(reference_key or "").strip()
    res_ads, ads = _fetch_ads(api_key, secret_key, cid, str(adgroup_id))
    if res_ads.status_code != 200:
        return ""
    before_ids = {str(x).strip() for x in (before_ids or set()) if str(x).strip()}
    for ad in (ads or []):
        if not isinstance(ad, dict):
            continue
        ad_id = _extract_ad_id(ad)
        if before_ids and ad_id in before_ids:
            continue
        if _extract_reference_key_from_ad(ad) == reference_key:
            return ad_id
    return ""
def _build_copy_summary(source_adgroup_id: str, target_adgroup_id: str) -> Dict[str, Any]:
    return {
        "source_adgroup_id": str(source_adgroup_id or "").strip(),
        "target_adgroup_id": str(target_adgroup_id or "").strip(),
        "strategy": "none",
        "keywords": {"source": 0, "success": 0, "fail": 0},
        "ads": {"source": 0, "success": 0, "fail": 0, "standard_source": 0, "shopping_source": 0},
        "group_extensions": {"source": 0, "success": 0, "fail": 0},
        "ad_extensions": {"source": 0, "success": 0, "fail": 0},
        "negatives": {"source": 0, "success": 0, "fail": 0},
        "notes": [],
    }
def _format_copy_summary(summary: Dict[str, Any]) -> str:
    if not isinstance(summary, dict):
        return "복사 요약 없음"
    strategy = str(summary.get("strategy") or "none")
    kw = summary.get("keywords") or {}
    ads = summary.get("ads") or {}
    gext = summary.get("group_extensions") or {}
    aext = summary.get("ad_extensions") or {}
    neg = summary.get("negatives") or {}
    notes = [str(x).strip() for x in (summary.get("notes") or []) if str(x).strip()]
    base = (
        f"전략={strategy} | 키워드 {int(kw.get('success') or 0)}/{int(kw.get('source') or 0)}"
        f" | 소재 {int(ads.get('success') or 0)}/{int(ads.get('source') or 0)}"
        f" (일반 {int(ads.get('standard_source') or 0)}, 쇼핑 {int(ads.get('shopping_source') or 0)})"
        f" | 그룹확장 {int(gext.get('success') or 0)}/{int(gext.get('source') or 0)}"
        f" | 소재하위확장 {int(aext.get('success') or 0)}/{int(aext.get('source') or 0)}"
        f" | 제외검색어 {int(neg.get('success') or 0)}/{int(neg.get('source') or 0)}"
    )
    if notes:
        base += " | 참고: " + "; ".join(notes[:3])
    return base
def _copy_ad_owner_extensions(api_key: str, secret_key: str, cid: str, old_owner_id: str, new_owner_id: str, biz_channel_id: str | None = None) -> List[str]:
    errors: List[str] = []
    old_owner_id = str(old_owner_id or "").strip()
    new_owner_id = str(new_owner_id or "").strip()
    if not old_owner_id or not new_owner_id:
        return errors
    r_ext = _do_req("GET", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": old_owner_id})
    if r_ext.status_code != 200:
        if r_ext.status_code not in [404]:
            errors.append(f"소재 확장소재 조회 실패({old_owner_id}): {r_ext.text}")
        return errors
    for ext in (r_ext.json() or []):
        if not isinstance(ext, dict):
            continue
        item = _build_copy_extension_payload(api_key, secret_key, cid, ext, new_owner_id, biz_channel_id)
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": new_owner_id}, json_body=item)
        if res.status_code not in [200, 201] and "4003" not in res.text:
            ext_type = _normalize_extension_type(item.get("type"))
            errors.append(f"소재 확장소재 에러({ext_type}): {res.text}")
    return errors
def _copy_adgroup_children(api_key, secret_key, cid, old_adg_id, new_adg_id, biz_channel_id, include_keywords=True, include_ads=True, include_extensions=True, include_negatives=True, return_summary=False):
    errors: List[str] = []
    summary = _build_copy_summary(str(old_adg_id), str(new_adg_id))
    if include_keywords:
        r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": old_adg_id})
    else:
        r_kw = None
    if r_kw is not None and r_kw.status_code == 200:
        new_kws = []
        for kw in r_kw.json() or []:
            item = copy.deepcopy(kw)
            for k in ['nccKeywordId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']:
                item.pop(k, None)
            item.update({"nccAdgroupId": str(new_adg_id), "customerId": int(cid)})
            new_kws.append(item)
        summary["keywords"]["source"] = len(new_kws)
        if new_kws:
            for i in range(0, len(new_kws), 100):
                batch = new_kws[i:i + 100]
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=batch)
                if res.status_code in [200, 201]:
                    summary["keywords"]["success"] += len(batch)
                else:
                    for item in batch:
                        r_single = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=item)
                        if r_single.status_code in [200, 201]:
                            summary["keywords"]["success"] += 1
                        else:
                            summary["keywords"]["fail"] += 1
                            errors.append(f"키워드 에러: {r_single.text}")
    elif r_kw is not None and r_kw.status_code not in [200, 404]:
        errors.append(f"키워드 조회 에러: {r_kw.text}")
        summary["notes"].append("키워드 조회 실패")
    if include_ads:
        r_ad = _do_req("GET", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": old_adg_id})
    else:
        r_ad = None
    copied_ad_owner_pairs: List[Tuple[str, str]] = []
    if r_ad is not None and r_ad.status_code == 200:
        ads = [x for x in (r_ad.json() or []) if isinstance(x, dict)]
        summary["strategy"] = _classify_copy_ad_strategy(ads)
        summary["ads"]["source"] = len(ads)
        summary["ads"]["shopping_source"] = sum(1 for ad in ads if _normalize_ad_type(ad.get("type") or ad.get("adType")) in SHOPPING_AD_TYPES)
        summary["ads"]["standard_source"] = int(summary["ads"]["source"] or 0) - int(summary["ads"]["shopping_source"] or 0)
        target_signatures_before = _collect_ad_signatures(api_key, secret_key, cid, str(new_adg_id))
        expected_payloads: List[Dict[str, Any]] = []
        def _post_ad(ad):
            src_ad = ad if isinstance(ad, dict) else {}
            src_ad_id = _extract_ad_id(src_ad)
            item = _build_copy_ad_payload(api_key, secret_key, cid, src_ad, str(new_adg_id))
            ad_type = _normalize_ad_type(item.get("type"))
            item.setdefault("userLock", False)
            expected_payloads.append(copy.deepcopy(item))
            before_ids: set[str] = set()
            ref_key = _extract_reference_key_from_ad(src_ad) or str(item.get("referenceKey") or "").strip()
            if ad_type in SHOPPING_AD_TYPES:
                res_before, before_ads = _fetch_ads(api_key, secret_key, cid, str(new_adg_id))
                if res_before.status_code == 200:
                    before_ids = {_extract_ad_id(x) for x in (before_ads or []) if isinstance(x, dict) and _extract_ad_id(x)}
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id, "isList": "true"}, json_body=[item])
            else:
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id}, json_body=item)
            if res.status_code not in [200, 201]:
                ad_name = str(((item.get("ad") or {}).get("headline") if isinstance(item.get("ad"), dict) else "") or item.get("type") or "")
                return {"error": f"소재 에러({ad_name}): {res.text}", "source_ad_id": src_ad_id, "created_ad_id": ""}
            created_ad_id = _extract_created_ad_id_from_response(res)
            if not created_ad_id and ref_key:
                created_ad_id = _find_created_ad_id_by_reference(api_key, secret_key, cid, str(new_adg_id), ref_key, before_ids)
            if ad_type in SHOPPING_AD_TYPES and not created_ad_id:
                ad_name = str(item.get("type") or "쇼핑소재")
                return {"error": f"소재 에러({ad_name}): 생성 후 소재 ID 확인 실패", "source_ad_id": src_ad_id, "created_ad_id": ""}
            return {"error": None, "source_ad_id": src_ad_id, "created_ad_id": created_ad_id}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_post_ad, ad) for ad in ads]
            for f in as_completed(futures):
                result = f.result() or {}
                err_msg = result.get("error")
                if err_msg:
                    summary["ads"]["fail"] += 1
                    errors.append(err_msg)
                    continue
                summary["ads"]["success"] += 1
                src_ad_id = str(result.get("source_ad_id") or "").strip()
                created_ad_id = str(result.get("created_ad_id") or "").strip()
                if src_ad_id and created_ad_id:
                    copied_ad_owner_pairs.append((src_ad_id, created_ad_id))
        target_signatures_after = _collect_ad_signatures(api_key, secret_key, cid, str(new_adg_id))
        missing_payloads = []
        for payload in expected_payloads:
            sig = _make_ad_signature(payload)
            if sig and sig not in target_signatures_after:
                missing_payloads.append(payload)
        if missing_payloads:
            retry_success, retry_errors = _retry_missing_ads(
                api_key, secret_key, cid, str(new_adg_id), missing_payloads,
                existing_signatures=(target_signatures_after or target_signatures_before),
            )
            summary["ads"]["success"] += int(retry_success or 0)
            summary["ads"]["fail"] += len(retry_errors)
            errors.extend(retry_errors)
            if retry_success:
                summary["notes"].append(f"누락 소재 재시도 성공: {retry_success}건")
    elif r_ad is not None and r_ad.status_code not in [200, 404]:
        errors.append(f"소재 조회 에러: {r_ad.text}")
        summary["notes"].append("소재 조회 실패")
    if include_extensions:
        r_ext = _do_req("GET", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": old_adg_id})
    else:
        r_ext = None
    if r_ext is not None and r_ext.status_code == 200:
        group_exts = [x for x in (r_ext.json() or []) if isinstance(x, dict)]
        summary["group_extensions"]["source"] = len(group_exts)
        for ext in group_exts:
            item = _build_copy_extension_payload(api_key, secret_key, cid, ext, str(new_adg_id), biz_channel_id)
            res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": new_adg_id}, json_body=item)
            if res.status_code not in [200, 201] and "4003" not in res.text:
                summary["group_extensions"]["fail"] += 1
                ext_type = _normalize_extension_type(item.get("type"))
                errors.append(f"확장소재 에러({ext_type}): {res.text}")
            else:
                summary["group_extensions"]["success"] += 1
    elif r_ext is not None and r_ext.status_code not in [200, 404]:
        errors.append(f"광고그룹 확장소재 조회 에러: {r_ext.text}")
        summary["notes"].append("광고그룹 확장소재 조회 실패")
    if include_extensions and copied_ad_owner_pairs:
        seen_owner_pairs = set()
        for old_owner_id, new_owner_id in copied_ad_owner_pairs:
            pair = (str(old_owner_id), str(new_owner_id))
            if not pair[0] or not pair[1] or pair in seen_owner_pairs:
                continue
            seen_owner_pairs.add(pair)
            old_res, old_items = _fetch_extensions(api_key, secret_key, cid, pair[0])
            if old_res.status_code == 200:
                summary["ad_extensions"]["source"] += len([x for x in (old_items or []) if isinstance(x, dict)])
            child_errors = _copy_ad_owner_extensions(api_key, secret_key, cid, pair[0], pair[1], biz_channel_id)
            if child_errors:
                summary["ad_extensions"]["fail"] += len(child_errors)
                errors.extend(child_errors)
            elif old_res.status_code == 200:
                summary["ad_extensions"]["success"] += len([x for x in (old_items or []) if isinstance(x, dict)])
    if summary["ad_extensions"]["source"] < summary["ad_extensions"]["success"]:
        summary["ad_extensions"]["source"] = summary["ad_extensions"]["success"]
    if include_negatives:
        res_rk, rows_rk = _fetch_restricted_keywords(api_key, secret_key, cid, str(old_adg_id))
        if res_rk.status_code == 200 and rows_rk:
            post_rows = []
            for rk in rows_rk:
                if not isinstance(rk, dict):
                    continue
                kw = str(rk.get("keyword") or rk.get("restrictedKeyword") or "").strip()
                if not kw:
                    continue
                post_rows.append({
                    "nccAdgroupId": str(new_adg_id),
                    "customerId": int(cid),
                    "keyword": kw,
                    "type": _normalize_negative_type(rk.get("type") or rk.get("matchType") or 1),
                })
            summary["negatives"]["source"] = len(post_rows)
            if post_rows:
                neg_success, neg_fail, neg_results = _bulk_create_restricted_keywords(api_key, secret_key, cid, post_rows)
                summary["negatives"]["success"] = int(neg_success or 0)
                summary["negatives"]["fail"] = int(neg_fail or 0)
                if neg_fail > 0:
                    fail_msgs = [str(x.get("message") or "") for x in (neg_results or []) if not x.get("success")]
                    if fail_msgs:
                        errors.extend([f"제외검색어 에러: {msg}" for msg in fail_msgs[:10]])
                    else:
                        errors.append("제외검색어 에러: 일부 제외검색어 복사 실패")
        elif res_rk.status_code not in [200, 404]:
            errors.append(f"제외검색어 조회 에러: {res_rk.text}")
            summary["notes"].append("제외검색어 조회 실패")
    dedup_errors = list(dict.fromkeys([str(x) for x in errors if str(x).strip()]))
    if return_summary:
        return dedup_errors, summary
    return dedup_errors
def _extract_adgroup(src, target_camp_id, cid, biz_channel_id):
    res = {
        "nccCampaignId": str(target_camp_id),
        "customerId": int(cid),
        "name": src.get("name"),
        "adgroupType": src.get("adgroupType"),
        "useDailyBudget": src.get("useDailyBudget", False),
        "dailyBudget": src.get("dailyBudget", 0),
        "bidAmt": src.get("bidAmt", 70),
    }
    adgroup_type = src.get("adgroupType", "")
    if biz_channel_id and biz_channel_id not in ["keep", "undefined"] and adgroup_type == "WEB_SITE":
        res["pcChannelId"] = res["mobileChannelId"] = str(biz_channel_id)
    else:
        if src.get("pcChannelId"):
            res["pcChannelId"] = str(src.get("pcChannelId"))
        if src.get("mobileChannelId"):
            res["mobileChannelId"] = str(src.get("mobileChannelId"))
    for k in ["useStoreUrl", "nccProductGroupId", "contentsNetworkBidAmt", "keywordPlusFlag", "contractId", "adgroupAttrJson"]:
        if k in src and src.get(k) is not None:
            res[k] = src[k]
    if str(src.get("adgroupType") or "").upper() == "WEB_SITE":
        # 확장검색/예산비율은 광고그룹 생성 payload에도 최대한 포함하고,
        # 생성 후 _copy_adgroup_search_option_settings에서 한 번 더 검증/보정한다.
        for k in list(_EXPANDED_SEARCH_RESPONSE_KEYS) + list(_EXPANDED_SEARCH_BUDGET_RATIO_KEYS) + ["useKeywordPlus", "keywordPlusWeight", "useCloseVariant"]:
            if k in src and src.get(k) is not None:
                res[k] = src.get(k)
    return res
def _delete_entity_by_id(api_key: str, secret_key: str, cid: str, entity_type: str, entity_id: str):
    entity_id = str(entity_id or "").strip()
    if not entity_id:
        return _make_fake_response(400, "ID가 없습니다.")
    uri_map = {
        "campaign": f"/ncc/campaigns/{entity_id}",
        "adgroup": f"/ncc/adgroups/{entity_id}",
        "keyword": f"/ncc/keywords/{entity_id}",
        "ad": f"/ncc/ads/{entity_id}",
        "ad_extension": f"/ncc/ad-extensions/{entity_id}",
    }
    uri = uri_map.get(entity_type)
    if not uri:
        return _make_fake_response(400, f"지원하지 않는 삭제 유형: {entity_type}")
    return _do_req("DELETE", api_key, secret_key, cid, uri)
@app.route("/")
def index():
    accounts = []
    csv_path = os.path.join(BASE_DIR, "accounts.csv")
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(csv_path, encoding="cp949")
        cols = {c.lower().strip(): c for c in df.columns}
        cid_col = cols.get("customer_id") or cols.get("customerid") or df.columns[0]
        name_col = cols.get("account_name") or cols.get("name") or (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        df2 = df[[cid_col, name_col]].copy()
        df2.columns = ["customer_id", "account_name"]
        df2["customer_id"] = df2["customer_id"].astype(str).str.strip()
        df2["account_name"] = df2["account_name"].astype(str).str.strip()
        accounts = df2.to_dict(orient="records")
    return render_template("index.html", accounts=accounts)
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "naver-bulk-manager-refresh"})
app.register_blueprint(create_lookup_blueprint(lookup_service=_LOOKUP_SERVICE, target_object_func=_fetch_target_object))
_DETAIL_LOOKUP_SERVICE = DetailLookupService(
    fetch_keywords_func=_fetch_keywords,
    fetch_ads_func=_fetch_ads,
    fetch_extensions_func=_fetch_extensions,
    fetch_restricted_keywords_func=_fetch_restricted_keywords,
    keyword_matches_search_func=_keyword_matches_search,
)
app.register_blueprint(create_detail_lookup_blueprint(_DETAIL_LOOKUP_SERVICE))
_ACCOUNT_LOOKUP_SERVICE = AccountLookupService(
    normalize_lookup_scope_func=_normalize_lookup_scope,
    collect_asset_scope_adgroups_func=_collect_asset_scope_adgroups,
    collect_lookup_ads_func=_collect_lookup_ads_for_contexts,
    collect_lookup_extensions_func=_collect_lookup_extensions_for_contexts,
    collect_lookup_keywords_func=_collect_lookup_keywords_for_contexts,
    build_asset_lookup_workbook_func=_build_asset_lookup_workbook,
    workbook_to_bytesio_func=workbook_to_bytesio,
    xlsx_mime=XLSX_MIME,
)
app.register_blueprint(create_account_lookup_blueprint(_ACCOUNT_LOOKUP_SERVICE))
def get_keywords():
    d = request.json or {}
    res, rows = _fetch_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_id"))
    if res.status_code == 200:
        base_rows = rows or []
        search_text = str(d.get("keyword_search") or "").strip()
        match_mode = str(d.get("keyword_match_mode") or "partial").strip().lower()
        exact_match = match_mode == "exact"
        if search_text:
            filtered_rows = [
                row for row in base_rows
                if _keyword_matches_search(
                    row.get("keyword") or row.get("keywordNm") or row.get("keywordName") or row.get("name") or "",
                    search_text,
                    exact_match=exact_match,
                )
            ]
        else:
            filtered_rows = base_rows
        preview_limit_raw = d.get("preview_limit")
        if preview_limit_raw is not None:
            try:
                preview_limit = max(0, int(preview_limit_raw))
            except Exception:
                preview_limit = 10
            total_count = len(filtered_rows or [])
            preview_rows = (filtered_rows or [])[:preview_limit]
            return jsonify({
                "rows": preview_rows,
                "total_count": total_count,
                "total_unfiltered_count": len(base_rows or []),
                "truncated": total_count > len(preview_rows),
                "preview_limit": preview_limit,
                "keyword_search": search_text,
                "keyword_match_mode": "exact" if exact_match else "partial",
            })
        return jsonify(filtered_rows)
    return jsonify({"error": "키워드 조회 실패", "details": res.text}), 400
def get_ads():
    d = request.json or {}
    res, rows = _fetch_ads(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "소재 조회 실패", "details": res.text}), 400
def get_ad_extensions():
    d = request.json or {}
    res, rows = _fetch_extensions(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("owner_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "확장소재 조회 실패", "details": res.text}), 400
def query_account_ads():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    scope = _normalize_lookup_scope(d.get("scope") or d.get("search_scope"))
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    res_ctx, contexts, warnings, _ = _collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids)
    if res_ctx.status_code != 200:
        return jsonify({"error": "소재 조회 실패", "details": res_ctx.text}), 400
    rows, row_warnings = _collect_lookup_ads_for_contexts(api_key, secret_key, cid, contexts)
    warnings.extend(row_warnings)
    return jsonify({
        "ok": True,
        "scope": scope,
        "total": len(rows),
        "preview_limit": 200,
        "rows": rows,
        "warnings": warnings[:20],
        "message": f"소재 {len(rows):,}건 조회 완료",
    })
def query_account_extensions():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    scope = _normalize_lookup_scope(d.get("scope") or d.get("search_scope"))
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    res_ctx, contexts, warnings, _ = _collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids)
    if res_ctx.status_code != 200:
        return jsonify({"error": "확장소재 조회 실패", "details": res_ctx.text}), 400
    ad_rows, ad_warnings = _collect_lookup_ads_for_contexts(api_key, secret_key, cid, contexts)
    warnings.extend(ad_warnings)
    rows, row_warnings = _collect_lookup_extensions_for_contexts(api_key, secret_key, cid, contexts, ad_rows=ad_rows)
    warnings.extend(row_warnings)
    return jsonify({
        "ok": True,
        "scope": scope,
        "total": len(rows),
        "preview_limit": 200,
        "rows": rows,
        "warnings": warnings[:20],
        "message": f"확장소재 {len(rows):,}건 조회 완료",
    })
def query_account_keywords():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    scope = _normalize_lookup_scope(d.get("scope") or d.get("search_scope"))
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    res_ctx, contexts, warnings, _ = _collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids)
    if res_ctx.status_code != 200:
        return jsonify({"error": "키워드 조회 실패", "details": res_ctx.text}), 400
    rows, row_warnings = _collect_lookup_keywords_for_contexts(api_key, secret_key, cid, contexts)
    warnings.extend(row_warnings)
    return jsonify({
        "ok": True,
        "scope": scope,
        "total": len(rows),
        "preview_limit": 200,
        "rows": rows,
        "warnings": warnings[:20],
        "message": f"등록 키워드 {len(rows):,}건 조회 완료",
    })

def export_account_keywords_excel():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    scope = _normalize_lookup_scope(d.get("scope") or d.get("search_scope"))
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    res_ctx, contexts, warnings, _ = _collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids)
    if res_ctx.status_code != 200:
        return jsonify({"error": "키워드 엑셀 다운로드 실패", "details": res_ctx.text}), 400
    rows, row_warnings = _collect_lookup_keywords_for_contexts(api_key, secret_key, cid, contexts)
    warnings.extend(row_warnings)
    if not rows:
        return jsonify({"error": "내보낼 등록 키워드가 없습니다."}), 400
    scope_label = "선택 캠페인만" if scope == "campaign" else "계정 전체"
    wb = _build_asset_lookup_workbook(rows, "계정 등록 키워드 조회", scope_label, [
        ("campaignName", "캠페인명"), ("adgroupName", "광고그룹명"), ("keyword", "키워드"), ("status", "상태"),
        ("bidAmt", "입찰가"), ("useGroupBidAmt", "그룹입찰가사용"), ("matchType", "매치유형"),
        ("keywordId", "키워드 ID"), ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
    ])
    output = workbook_to_bytesio(wb)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    return send_file(output, mimetype=XLSX_MIME, as_attachment=True, download_name=f"account_keywords_{scope}_{stamp}.xlsx")

def export_account_ads_excel():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    scope = _normalize_lookup_scope(d.get("scope") or d.get("search_scope"))
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    res_ctx, contexts, warnings, _ = _collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids)
    if res_ctx.status_code != 200:
        return jsonify({"error": "소재 엑셀 다운로드 실패", "details": res_ctx.text}), 400
    rows, row_warnings = _collect_lookup_ads_for_contexts(api_key, secret_key, cid, contexts)
    warnings.extend(row_warnings)
    if not rows:
        return jsonify({"error": "내보낼 소재가 없습니다."}), 400
    scope_label = "선택 캠페인만" if scope == "campaign" else "계정 전체"
    wb = _build_asset_lookup_workbook(rows, "계정 등록 소재 조회", scope_label, [
        ("campaignType", "캠페인유형"), ("campaignName", "캠페인명"), ("adgroupType", "광고그룹유형"), ("adgroupName", "광고그룹명"),
        ("type", "소재유형"), ("status", "상태"),
        ("summary", "요약"), ("effectiveBidAmt", "적용입찰가"), ("adBidAmt", "소재입찰가"), ("adUseGroupBidAmt", "그룹입찰가사용"), ("adgroupBidAmt", "광고그룹입찰가"), ("adId", "소재 ID"), ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
    ])
    output = workbook_to_bytesio(wb)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    return send_file(output, mimetype=XLSX_MIME, as_attachment=True, download_name=f"account_ads_{scope}_{stamp}.xlsx")
def export_account_extensions_excel():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    scope = _normalize_lookup_scope(d.get("scope") or d.get("search_scope"))
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    res_ctx, contexts, warnings, _ = _collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids)
    if res_ctx.status_code != 200:
        return jsonify({"error": "확장소재 엑셀 다운로드 실패", "details": res_ctx.text}), 400
    ad_rows, ad_warnings = _collect_lookup_ads_for_contexts(api_key, secret_key, cid, contexts)
    warnings.extend(ad_warnings)
    rows, row_warnings = _collect_lookup_extensions_for_contexts(api_key, secret_key, cid, contexts, ad_rows=ad_rows)
    warnings.extend(row_warnings)
    if not rows:
        return jsonify({"error": "내보낼 확장소재가 없습니다."}), 400
    scope_label = "선택 캠페인만" if scope == "campaign" else "계정 전체"
    wb = _build_asset_lookup_workbook(rows, "계정 등록 확장소재 조회", scope_label, [
        ("campaignType", "캠페인유형"), ("campaignName", "캠페인명"), ("adgroupType", "광고그룹유형"), ("adgroupName", "광고그룹명"),
        ("ownerScope", "적용대상"), ("adExtensionId", "확장소재 ID"),
        ("imageId", "이미지 ID"), ("type", "확장소재유형"), ("status", "상태"), ("summary", "요약"), ("ownerId", "owner ID"),
        ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
    ])
    output = workbook_to_bytesio(wb)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    return send_file(output, mimetype=XLSX_MIME, as_attachment=True, download_name=f"account_extensions_{scope}_{stamp}.xlsx")
def get_restricted_keywords():
    d = request.json or {}
    res, rows = _fetch_restricted_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "제외키워드 조회 실패", "details": res.text}), 400
@app.route("/find_powerlink_duplicate_keywords", methods=["POST"])
def find_powerlink_duplicate_keywords():
    d = request.json or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
    duplicate_scope = str(d.get("duplicate_scope") or "selected_campaigns").strip()
    target_scope = str(d.get("target_scope") or d.get("search_scope") or "selected_campaigns").strip().lower()
    if target_scope not in {"selected_campaigns", "selected_adgroups"}:
        target_scope = "selected_adgroups" if adgroup_ids else "selected_campaigns"
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    if target_scope == "selected_adgroups" and not adgroup_ids:
        return jsonify({"error": "선택 그룹내 조회를 사용하려면 좌측에서 광고그룹을 체크해주세요."}), 400
    if target_scope == "selected_campaigns" and not campaign_ids:
        return jsonify({"error": "캠페인을 1개 이상 선택해주세요."}), 400
    try:
        result = _find_powerlink_duplicate_keywords(
            api_key, secret_key, cid, campaign_ids,
            duplicate_scope=duplicate_scope,
            adgroup_ids=adgroup_ids,
            target_scope=target_scope,
        )
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": "중복 키워드 조회 실패", "details": str(e)}), 400
@app.route("/find_account_powerlink_duplicate_keywords", methods=["POST"])
def find_account_powerlink_duplicate_keywords():
    d = request.json or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    try:
        result = _find_account_powerlink_duplicate_keywords(api_key, secret_key, cid)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": "계정 중복 키워드 조회 실패", "details": str(e)}), 400
@app.route("/export_powerlink_duplicate_keywords_excel", methods=["POST"])
def export_powerlink_duplicate_keywords_excel():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
    duplicate_scope = str(d.get("duplicate_scope") or "selected_campaigns").strip()
    target_scope = str(d.get("target_scope") or d.get("search_scope") or "selected_campaigns").strip().lower()
    if target_scope not in {"selected_campaigns", "selected_adgroups"}:
        target_scope = "selected_adgroups" if adgroup_ids else "selected_campaigns"
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    if target_scope == "selected_adgroups" and not adgroup_ids:
        return jsonify({"error": "선택 그룹내 조회를 사용하려면 좌측에서 광고그룹을 체크해주세요."}), 400
    if target_scope == "selected_campaigns" and not campaign_ids:
        return jsonify({"error": "캠페인을 1개 이상 선택해주세요."}), 400
    try:
        result = _find_powerlink_duplicate_keywords(
            api_key, secret_key, cid, campaign_ids,
            duplicate_scope=duplicate_scope,
            adgroup_ids=adgroup_ids,
            target_scope=target_scope,
        )
        rows = result.get("rows") or []
        if not rows:
            return jsonify({"error": "내보낼 중복 키워드가 없습니다.", "details": result.get("message") or "중복 키워드가 없습니다."}), 400
        scope_label = result.get("duplicate_scope_label") or result.get("target_scope_label") or "선택 캠페인"
        wb = _build_powerlink_duplicate_keyword_workbook(result, scope_label=scope_label)
        output = workbook_to_bytesio(wb)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        return send_file(output, mimetype=XLSX_MIME, as_attachment=True, download_name=f"powerlink_duplicate_keywords_{stamp}.xlsx")
    except Exception as e:
        return jsonify({"error": "중복 키워드 엑셀 다운로드 실패", "details": str(e)}), 400
@app.route("/get_powerlink_keyword_stats", methods=["POST"])
def get_powerlink_keyword_stats():
    d = request.json or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    campaign_ids = d.get("campaign_ids") or []
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    try:
        result = _get_powerlink_keyword_stats(api_key, secret_key, cid, campaign_ids)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": "파워링크 키워드 개수 조회 실패", "details": str(e)}), 400
@app.route("/search_powerlink_keywords", methods=["POST"])
def search_powerlink_keywords():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    search_text = str(d.get("search_text") or d.get("keyword_query") or "").strip()
    exact_match = _boolish(d.get("exact_match"), False)
    exclude_text = str(d.get("exclude_text") or d.get("exclude_keyword_query") or "").strip()
    search_scope = str(d.get("search_scope") or "account").strip().lower()
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
    adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    if not search_text:
        return jsonify({"error": "검색어를 입력해주세요."}), 400
    if search_scope not in {"account", "selected_campaigns", "selected_adgroups"}:
        search_scope = "account"
    if search_scope == "selected_campaigns" and not campaign_ids:
        return jsonify({"error": "선택 캠페인 조회를 사용하려면 좌측에서 캠페인을 체크해주세요."}), 400
    if search_scope == "selected_adgroups" and not adgroup_ids:
        return jsonify({"error": "선택 그룹내 조회를 사용하려면 좌측에서 광고그룹을 체크해주세요."}), 400
    try:
        scan = _scan_powerlink_keywords_by_search(
            api_key,
            secret_key,
            cid,
            search_text,
            exact_match=exact_match,
            campaign_ids=(campaign_ids if search_scope == "selected_campaigns" else None),
            adgroup_ids=(adgroup_ids if search_scope == "selected_adgroups" else None),
            exclude_text=exclude_text,
        )
        if search_scope == "selected_campaigns":
            selected_set = set(campaign_ids)
            filtered_rows = [row for row in (scan.get("matched_rows") or []) if str(row.get("campaign_id") or "").strip() in selected_set]
            scan = dict(scan)
            scan["matched_rows"] = filtered_rows
            scan["matched_count"] = len(filtered_rows)
            scan["powerlink_campaigns"] = [row for row in (scan.get("powerlink_campaigns") or []) if str(row.get("id") or "").strip() in selected_set]
            scan["adgroup_contexts"] = [row for row in (scan.get("adgroup_contexts") or []) if str(row.get("campaign_id") or "").strip() in selected_set]
        elif search_scope == "selected_adgroups":
            selected_adgroup_set = set(adgroup_ids)
            filtered_rows = [row for row in (scan.get("matched_rows") or []) if str(row.get("adgroup_id") or "").strip() in selected_adgroup_set]
            scan = dict(scan)
            scan["matched_rows"] = filtered_rows
            scan["matched_count"] = len(filtered_rows)
            scan["adgroup_contexts"] = [row for row in (scan.get("adgroup_contexts") or []) if str(row.get("adgroup_id") or "").strip() in selected_adgroup_set]
            selected_campaign_set = {str(row.get("campaign_id") or "").strip() for row in (scan.get("adgroup_contexts") or []) if str(row.get("campaign_id") or "").strip()}
            scan["powerlink_campaigns"] = [row for row in (scan.get("powerlink_campaigns") or []) if str(row.get("id") or "").strip() in selected_campaign_set]
        preview_token = hashlib.sha256(f"{cid}|{search_scope}|{'/'.join(campaign_ids)}|{'/'.join(adgroup_ids)}|{search_text}|{exclude_text}|{'1' if exact_match else '0'}|{int(scan.get('matched_count') or 0)}".encode('utf-8')).hexdigest()[:20]
        scope_label = "선택 캠페인 기준" if search_scope == "selected_campaigns" else ("선택 그룹 기준" if search_scope == "selected_adgroups" else "계정 전체 기준")
        return jsonify({
            "ok": True,
            "message": f"[{scope_label}]\n" + _build_powerlink_keyword_search_message(search_text, exact_match, scan, row_preview_limit=80, exclude_text=exclude_text),
            "search_text": search_text,
            "search_groups": scan.get("search_groups") or _parse_keyword_search_groups(search_text),
            "exact_match": bool(exact_match),
            "exclude_text": exclude_text,
            "matched_count": int(scan.get("matched_count") or 0),
            "scanned_keyword_count": int(scan.get("scanned_keyword_count") or 0),
            "total_powerlink_campaign_count": len(scan.get("powerlink_campaigns") or []),
            "total_powerlink_adgroup_count": len(scan.get("adgroup_contexts") or []),
            "rows": (scan.get("matched_rows") or []),
            "warnings": (scan.get("warnings") or [])[:10],
            "preview_token": preview_token,
            "search_scope": search_scope,
            "selected_campaign_count": len(campaign_ids) if search_scope == "selected_campaigns" else 0,
            "selected_adgroup_count": len(adgroup_ids) if search_scope == "selected_adgroups" else 0,
        })
    except Exception as e:
        return jsonify({"error": "검색어 기준 키워드 조회 실패", "details": str(e)}), 400
@app.route("/export_powerlink_keywords_excel", methods=["POST"])
def export_powerlink_keywords_excel():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    search_text = str(d.get("search_text") or d.get("keyword_query") or "").strip()
    exact_match = _boolish(d.get("exact_match"), False)
    exclude_text = str(d.get("exclude_text") or d.get("exclude_keyword_query") or "").strip()
    search_scope = str(d.get("search_scope") or "account").strip().lower()
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API 정보 및 광고주를 선택해주세요."}), 400
    if not search_text:
        return jsonify({"error": "검색어를 입력해주세요."}), 400
    if search_scope not in {"account", "selected_campaigns", "selected_adgroups"}:
        search_scope = "account"
    if search_scope == "selected_campaigns" and not campaign_ids:
        return jsonify({"error": "선택 캠페인 조회를 사용하려면 좌측에서 캠페인을 체크해주세요."}), 400
    if search_scope == "selected_adgroups" and not adgroup_ids:
        return jsonify({"error": "선택 그룹내 조회를 사용하려면 좌측에서 광고그룹을 체크해주세요."}), 400
    try:
        scan = _scan_powerlink_keywords_by_search(
            api_key,
            secret_key,
            cid,
            search_text,
            exact_match=exact_match,
            campaign_ids=(campaign_ids if search_scope == "selected_campaigns" else None),
            adgroup_ids=(adgroup_ids if search_scope == "selected_adgroups" else None),
            exclude_text=exclude_text,
        )
        rows = scan.get("matched_rows") or []
        if not rows:
            return jsonify({"error": "내보낼 일치 키워드가 없습니다."}), 400
        wb = _build_powerlink_keyword_export_workbook(rows, search_text, exact_match, exclude_text=exclude_text)
        output = workbook_to_bytesio(wb)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        mode_label = "exact" if exact_match else "partial"
        filename = f"powerlink_keyword_search_{mode_label}_{stamp}.xlsx"
        return send_file(
            output,
            mimetype=XLSX_MIME,
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        return jsonify({"error": "키워드 엑셀 다운로드 실패", "details": str(e)}), 400
@app.route("/get_action_logs", methods=["GET"])
def get_action_logs():
    try:
        limit_raw = str(request.args.get("limit") or "120").strip()
        limit = int(limit_raw)
        if limit <= 0:
            limit = 120
        limit = min(limit, 500)
        return jsonify(_read_action_logs(limit))
    except Exception as e:
        return jsonify({"error": "작업 로그 조회 실패", "details": str(e)}), 500

@app.route("/export_action_logs_excel", methods=["GET"])
def export_action_logs_excel():
    try:
        limit_raw = str(request.args.get("limit") or str(_ACTION_LOG_MAX_LINES)).strip()
        try:
            limit = int(limit_raw)
        except Exception:
            limit = _ACTION_LOG_MAX_LINES
        if limit <= 0:
            limit = _ACTION_LOG_MAX_LINES
        limit = min(limit, _ACTION_LOG_MAX_LINES)

        rows = _read_action_logs(limit)
        if not rows:
            return jsonify({"error": "다운로드할 작업 로그가 없습니다."}), 404

        headers = [
            "번호", "일시", "상태", "HTTP상태", "작업", "광고주ID",
            "요약", "메시지", "요청경로"
        ]
        data_rows = []
        for idx, row in enumerate(rows, 1):
            status_raw = str(row.get("status") or "").strip().lower()
            status_label = "실패" if status_raw == "error" else "성공"
            data_rows.append([
                idx,
                row.get("ts") or "",
                status_label,
                row.get("http_status") or "",
                row.get("action") or "",
                row.get("customer_id") or "",
                row.get("summary") or "",
                row.get("message") or "",
                row.get("path") or "",
            ])

        widths = {
            "A": 8, "B": 20, "C": 10, "D": 12, "E": 24,
            "F": 18, "G": 46, "H": 58, "I": 36,
        }
        wb = build_table_workbook(
            sheet_title="작업이력로그",
            headers=headers,
            rows=data_rows,
            widths=widths,
            freeze_panes="A2",
            auto_filter=True,
            header_fill="E8F0FE",
            header_font_color=None,
        )

        bio = workbook_to_bytesio(wb)
        filename = f"action_logs_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            bio,
            mimetype=XLSX_MIME,
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        return jsonify({"error": "작업 로그 엑셀 다운로드 실패", "details": str(e)}), 500


@app.route("/clear_action_logs", methods=["POST"])
def clear_action_logs():
    try:
        with _ACTION_LOG_LOCK:
            os.makedirs(LOG_DIR, exist_ok=True)
            with open(ACTION_LOG_PATH, 'w', encoding='utf-8') as fp:
                fp.write("")
        return jsonify({"ok": True, "message": "작업 로그를 비웠습니다."})
    except Exception as e:
        return jsonify({"error": "작업 로그 비우기 실패", "details": str(e)}), 500
def create_campaign():
    d = request.json or {}
    payload = {
        "customerId": int(d.get("customer_id")),
        "name": str(d.get("name") or "").strip(),
        "campaignTp": _normalize_campaign_tp(d.get("campaign_tp")),
        "useDailyBudget": bool(d.get("use_daily_budget", True)),
        "dailyBudget": int(d.get("daily_budget") or 0),
    }
    if not payload["name"]:
        return jsonify({"error": "캠페인명은 필수입니다."}), 400
    res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/campaigns", json_body=payload)
    if res.status_code in [200, 201]:
        _cache_invalidate(d.get("api_key"), d.get("secret_key"), d.get("customer_id"))
        return jsonify({"ok": True, "item": res.json(), "message": "캠페인 생성 완료"})
    return jsonify({"error": "캠페인 생성 실패", "details": res.text}), 400
def create_adgroup_simple():
    d = request.json or {}
    campaign_id = str(d.get("campaign_id") or "").strip()
    name = str(d.get("name") or "").strip()
    requested_campaign_tp = _normalize_campaign_tp(d.get("campaign_tp"))
    requested_adgroup_tp = _normalize_adgroup_tp(d.get("adgroup_type"), requested_campaign_tp)
    biz_channel_id = str(d.get("biz_channel_id") or "").strip()
    media_type = str(d.get("media_type") or "ALL").strip().upper()
    media_detail = _normalize_media_detail(d.get("media_detail"))
    use_keyword_plus = d.get("use_keyword_plus")
    use_close_variant = d.get("use_close_variant")
    keyword_plus_weight = d.get("keyword_plus_weight")
    if not campaign_id or not name:
        return jsonify({"error": "캠페인과 광고그룹명은 필수입니다."}), 400
    campaign_detail = _fetch_campaign_detail(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), campaign_id)
    actual_campaign_tp = _normalize_campaign_tp((campaign_detail or {}).get("campaignTp") or requested_campaign_tp)
    # 중요: 최신 버전에서 쇼핑 캠페인 유형을 실제 campaignTp(CATALOG/SHOPPING_BRAND 등)로 강제 승격하면서
    # 기존 3/4번 파일 대비 생성 실패가 늘어날 수 있다. 생성 자체는 예전처럼 "요청값 우선"으로 시도하고,
    # 필요할 때만 실제 campaignTp 기반 fallback 을 추가로 시도한다.
    is_requested_shopping = _is_shopping_campaign_type(requested_campaign_tp) or _is_shopping_campaign_type(requested_adgroup_tp)
    is_actual_shopping = _is_shopping_campaign_type(actual_campaign_tp)
    is_web_campaign = not (is_requested_shopping or is_actual_shopping) and actual_campaign_tp == "WEB_SITE"
    legacy_adgroup_tp = _normalize_adgroup_tp(d.get("adgroup_type"), requested_campaign_tp or actual_campaign_tp)
    inferred_adgroup_tp = _normalize_adgroup_tp(d.get("adgroup_type"), actual_campaign_tp or requested_campaign_tp)
    def _base_payload(adgroup_tp: str, attach_biz_channel: bool) -> Dict[str, Any]:
        payload = {
            "customerId": int(d.get("customer_id")),
            "nccCampaignId": campaign_id,
            "name": name,
            "adgroupType": adgroup_tp,
            "useDailyBudget": bool(d.get("use_daily_budget", True)),
            "dailyBudget": int(d.get("daily_budget") or 0),
            "bidAmt": int(d.get("bid_amt") or 70),
        }
        if attach_biz_channel and biz_channel_id:
            payload["pcChannelId"] = biz_channel_id
            payload["mobileChannelId"] = biz_channel_id
        return payload
    attempts: List[Dict[str, Any]] = []
    candidate_payloads: List[Tuple[str, Dict[str, Any]]] = []
    if is_web_campaign:
        if not biz_channel_id:
            biz_channel_id = _fetch_first_biz_channel_id(d.get("api_key"), d.get("secret_key"), d.get("customer_id"))
            if not biz_channel_id:
                return jsonify({"error": "파워링크 광고그룹은 비즈채널이 필요합니다. 비즈채널을 먼저 선택하거나 생성해주세요."}), 400
        candidate_payloads.append(("web-primary", _base_payload(legacy_adgroup_tp, True)))
    else:
        seen_signatures = set()
        def _append_candidate(label: str, adgroup_tp: str):
            tp = str(adgroup_tp or "").strip().upper()
            if not tp:
                return
            payload = _base_payload(tp, False)
            sig = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            if sig in seen_signatures:
                return
            seen_signatures.add(sig)
            candidate_payloads.append((label, payload))
        # 1차: 예전 3/4번 파일처럼 요청값 우선(가장 호환성이 높음)
        _append_candidate("shopping-legacy", legacy_adgroup_tp)
        # 2차: 실제 캠페인 유형 기반 fallback. 다만 SHOPPING_BRAND 는 상품그룹 ID가 없으면 실패 확률이 높아 자동 강제하지 않는다.
        actual_upper = str(actual_campaign_tp or "").strip().upper()
        if actual_upper == "CATALOG":
            _append_candidate("shopping-catalog", "CATALOG")
        elif actual_upper == "SHOPPING_BRAND":
            if str(d.get("ncc_product_group_id") or d.get("nccProductGroupId") or "").strip():
                payload = _base_payload("SHOPPING_BRAND", False)
                payload["nccProductGroupId"] = str(d.get("ncc_product_group_id") or d.get("nccProductGroupId") or "").strip()
                if int(payload.get("bidAmt") or 0) < 300:
                    payload["bidAmt"] = 300
                sig = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                if sig not in seen_signatures:
                    seen_signatures.add(sig)
                    candidate_payloads.append(("shopping-brand", payload))
        elif inferred_adgroup_tp and inferred_adgroup_tp != legacy_adgroup_tp:
            _append_candidate("shopping-inferred", inferred_adgroup_tp)
    final_res = None
    final_payload = None
    final_label = ""
    for label, payload in candidate_payloads:
        attempts.append({"label": label, "payload": payload})
        res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/adgroups", json_body=payload)
        if res.status_code in [200, 201]:
            final_res = res
            final_payload = payload
            final_label = label
            break
        final_res = res
        final_payload = payload
        final_label = label
    res = final_res
    payload = final_payload or _base_payload(legacy_adgroup_tp, is_web_campaign and bool(biz_channel_id))
    if res and res.status_code in [200, 201]:
        item = res.json() or {}
        new_adgroup_id = str(item.get("nccAdgroupId") or item.get("id") or "").strip()
        warnings: List[str] = []
        if new_adgroup_id:
            ok_media_pm, detail_media_pm = _update_pc_mobile_target(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), new_adgroup_id, media_type)
            if not ok_media_pm:
                warnings.append(f"PC/모바일 매체 적용 실패: {detail_media_pm}")
            ok_media_network, detail_media_network = _update_media_target(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), new_adgroup_id, media_detail)
            if not ok_media_network:
                warnings.append(f"세부 매체 적용 실패: {detail_media_network}")
            if is_web_campaign:
                ukp = None if use_keyword_plus is None else _normalize_bool_for_api(use_keyword_plus, False)
                ucv = None if use_close_variant is None else _normalize_bool_for_api(use_close_variant, False)
                kwp = None
                if str(keyword_plus_weight or "").strip() != "":
                    try:
                        kwp = int(keyword_plus_weight)
                    except Exception:
                        kwp = None
                ok_opts, detail_opts = _update_adgroup_search_options(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), new_adgroup_id, use_keyword_plus=ukp, keyword_plus_weight=kwp, use_close_variant=ucv)
                if not ok_opts:
                    warnings.append(f"파워링크 검색옵션 적용 실패: {detail_opts}")
        message = "광고그룹 생성 완료"
        if warnings:
            message += " (일부 후처리 실패)"
        if final_label and final_label != "web-primary":
            message += f" [{final_label}]"
        return jsonify({"ok": True, "item": item, "message": message, "warnings": warnings})
    detail = res.text if res is not None else "알 수 없는 오류"
    try:
        j = res.json() if res is not None else {}
        detail = j.get("title") or j.get("message") or detail
        if j.get("detail"):
            detail = f"{detail} | {j.get('detail')}"
    except Exception:
        pass
    hint = None
    if str(actual_campaign_tp or "").strip().upper() == "SHOPPING_BRAND" and not str(d.get("ncc_product_group_id") or d.get("nccProductGroupId") or "").strip():
        hint = "현재 캠페인이 SHOPPING_BRAND 로 보입니다. 이 유형은 상품그룹 ID(nccProductGroupId) 없이는 생성이 실패할 수 있습니다."
    return jsonify({
        "error": "광고그룹 생성 실패",
        "details": detail,
        "payload": payload,
        "hint": hint,
        "debug": {
            "requested_campaign_tp": requested_campaign_tp,
            "actual_campaign_tp": actual_campaign_tp,
            "requested_adgroup_tp": requested_adgroup_tp,
            "legacy_adgroup_tp": legacy_adgroup_tp,
            "inferred_adgroup_tp": inferred_adgroup_tp,
            "biz_channel_id": biz_channel_id,
            "campaign_id": campaign_id,
            "is_web_campaign": is_web_campaign,
            "is_requested_shopping": is_requested_shopping,
            "is_actual_shopping": is_actual_shopping,
            "campaign_detail": campaign_detail,
            "attempted_payloads": attempts,
            "last_attempt_label": final_label,
        },
    }), 400
def create_keywords_simple():
    d = request.json or {}
    keyword_batches = d.get("keyword_batches") if isinstance(d.get("keyword_batches"), list) else None
    rows: List[Dict[str, Any]] = []
    batch_summary: List[Dict[str, Any]] = []
    errors: List[str] = []
    def _append_keywords(adgroup_id: str, keywords_text: str, slot_index: Optional[int] = None):
        adgroup_id_local = str(adgroup_id or "").strip()
        keywords_local = [x.strip() for x in str(keywords_text or "").replace(",", "\n").splitlines() if x.strip()]
        if not keywords_local:
            return
        if not adgroup_id_local:
            errors.append(f"광고그룹을 선택하지 않은 슬롯이 있습니다{'' if slot_index is None else f' (슬롯 {slot_index})'}.")
            return
        for kw in keywords_local:
            rows.append({
                "nccAdgroupId": adgroup_id_local,
                "keyword": kw,
                "useGroupBidAmt": bool(d.get("use_group_bid_amt", True)),
                "bidAmt": int(d.get("bid_amt") or 70),
                "userLock": bool(d.get("user_lock", False)),
            })
        batch_summary.append({
            "slot_index": int(slot_index or len(batch_summary) + 1),
            "adgroup_id": adgroup_id_local,
            "keyword_count": len(keywords_local),
        })
    if keyword_batches:
        for idx, batch in enumerate(keyword_batches, start=1):
            if not isinstance(batch, dict):
                continue
            _append_keywords(
                str(batch.get("adgroup_id") or "").strip(),
                batch.get("keywords") or "",
                int(batch.get("slot_index") or idx),
            )
    else:
        _append_keywords(str(d.get("adgroup_id") or "").strip(), d.get("keywords") or "", 1)
    if errors:
        return jsonify({"error": " / ".join(errors)}), 400
    if not rows:
        return jsonify({"error": "광고그룹과 키워드는 필수입니다."}), 400
    success, fail, results = _bulk_create_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), rows)
    return jsonify({
        "ok": True,
        "total": len(rows),
        "success": success,
        "fail": fail,
        "results": results,
        "batch_summary": batch_summary,
    })
def _parse_target_ids(payload: Dict[str, Any], single_key: str, multi_key: str) -> List[str]:
    ids: List[str] = []
    raw_multi = payload.get(multi_key)
    if isinstance(raw_multi, list):
        ids.extend([str(x).strip() for x in raw_multi if str(x).strip()])
    elif isinstance(raw_multi, str) and raw_multi.strip():
        ids.extend([x.strip() for x in raw_multi.replace(",", "\n").splitlines() if x.strip()])
    raw_single = str(payload.get(single_key) or "").strip()
    if raw_single:
        ids.append(raw_single)
    uniq: List[str] = []
    seen = set()
    for x in ids:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq
def _count_keyword_insertions(text_value: str) -> int:
    return len(re.findall(r"\{키워드(?::[^}]+)?\}", str(text_value or "")))
def _apply_keyword_insertion_template(text_value: str, replace_keyword: str) -> str:
    text_value = str(text_value or "").strip()
    if not text_value:
        return text_value
    if "{키워드}" in text_value:
        if not replace_keyword:
            raise ValueError("키워드삽입 사용 시 대체키워드를 입력해야 합니다.")
        text_value = text_value.replace("{키워드}", f"{{키워드:{replace_keyword}}}")
    return text_value
def _visible_keyword_length(text_value: str) -> int:
    text_value = str(text_value or "").strip()
    text_value = re.sub(r"\{키워드:[^}]+\}", "{키워드}", text_value)
    return len(text_value)
def _text_ad_length_errors(headline: str, description: str) -> List[str]:
    errors: List[str] = []
    headline = str(headline or "").strip()
    description = str(description or "").strip()
    headline_visible_len = _visible_keyword_length(headline)
    description_visible_len = _visible_keyword_length(description)
    if not (1 <= headline_visible_len <= 15):
        errors.append(f"제목은 대체키워드 제외 기준 1~15자여야 합니다. (현재 {headline_visible_len}자)")
    if not (20 <= description_visible_len <= 45):
        errors.append(f"설명은 대체키워드 제외 기준 20~45자여야 합니다. (현재 {description_visible_len}자)")
    return errors
def _parse_extension_position(value: Any) -> int | None:
    try:
        pos = int(str(value or "").strip())
    except Exception:
        return None
    return pos if pos in {1, 2} else None
def _normalize_headline_pin_input(value: Any) -> int | None:
    s = str(value or '').strip()
    if not s:
        return None
    lowered = s.lower().replace(' ', '')
    if lowered in {'all', '전체', '모든위치', '모든위치노출', '전체노출', 'allpositions'}:
        return None
    if lowered in {'1', '1번', '1st', 'pin1', 'position1', '위치1', '위치1만', '위치1만노출', '위치1에만노출', '위치1만노출가능'}:
        return 1
    if lowered in {'2', '2번', '2nd', 'pin2', 'position2', '위치2', '위치2만', '위치2만노출', '위치2에만노출', '위치2만노출가능'}:
        return 2
    m = re.search(r'([12])', lowered)
    if m:
        return int(m.group(1))
    raise ValueError('pin 값은 비우거나 1 / 2 로 입력해주세요.')
def _row_pick_value(row: Dict[str, Any], candidate_keys: List[str]) -> Any:
    lookup: Dict[str, Any] = {}
    for k, v in (row or {}).items():
        nk = re.sub(r"\s+", "", str(k or "")).lower()
        lookup[nk] = v
    for key in candidate_keys:
        nk = re.sub(r"\s+", "", str(key or "")).lower()
        if nk in lookup:
            return lookup[nk]
    return None
def _row_has_any_key(row: Dict[str, Any], candidate_keys: List[str]) -> bool:
    lookup = {re.sub(r"\s+", "", str(k or "")).lower() for k in (row or {}).keys()}
    for key in candidate_keys:
        nk = re.sub(r"\s+", "", str(key or "")).lower()
        if nk in lookup:
            return True
    return False
def _boolish(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'y', 'yes', 'on'}
def _create_text_ad_for_adgroup(d: Dict[str, Any], adgroup_id: str, headline: str, description: str, pc_url: str, mobile_url: str) -> Dict[str, Any]:
    payload = {
        "customerId": int(d.get("customer_id")),
        "nccAdgroupId": adgroup_id,
        "type": "TEXT_45",
        "userLock": bool(d.get("user_lock", False)),
        "ad": {
            "headline": headline,
            "description": description,
            "pc": {"final": pc_url, "display": _display_url_from_final(pc_url)},
            "mobile": {"final": mobile_url, "display": _display_url_from_final(mobile_url)},
        },
    }
    res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ads", params={"nccAdgroupId": adgroup_id}, json_body=payload)
    ok = res is not None and res.status_code in [200, 201]
    return {
        "ok": ok,
        "adgroup_id": adgroup_id,
        "detail": (res.text if res is not None else "응답 없음"),
        "item": (res.json() if ok else None),
    }
def _create_shopping_ad_for_adgroup(d: Dict[str, Any], adgroup_id: str, reference_key: str, ad_type: str) -> Dict[str, Any]:
    payload = {
        "customerId": int(d.get("customer_id")),
        "nccAdgroupId": adgroup_id,
        "type": ad_type,
        "referenceKey": reference_key,
        "useGroupBidAmt": bool(d.get("use_group_bid_amt", True)),
        "bidAmt": int(d.get("bid_amt") or 0),
        "userLock": bool(d.get("user_lock", False)),
        "ad": {},
    }
    res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ads", params={"nccAdgroupId": adgroup_id, "isList": "true"}, json_body=[payload])
    ok = res is not None and res.status_code in [200, 201]
    return {
        "ok": ok,
        "adgroup_id": adgroup_id,
        "detail": (res.text if res is not None else "응답 없음"),
        "item": (res.json() if ok else None),
    }
def _create_extension_for_owner(d: Dict[str, Any], owner_id: str, ext_type: str, data: Dict[str, Any], position: int | None = None) -> Dict[str, Any]:
    payload = _build_extension_payload(owner_id, ext_type, data, int(d.get("customer_id")), position=position)
    if ext_type != "SHOPPING_EXTRA" and not payload.get("adExtension"):
        return {"ok": False, "owner_id": owner_id, "detail": "확장소재 내용이 비어 있습니다."}
    res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
    ok = res is not None and res.status_code in [200, 201]
    return {
        "ok": ok,
        "owner_id": owner_id,
        "detail": (res.text if res is not None else "응답 없음"),
        "item": (res.json() if ok else None),
    }
def _bulk_upload_one_text_ad(api_key: str, secret_key: str, cid: str, row_no: int, adgroup_id: str, headline: str, description: str, pc_url: str, mobile_url: str) -> Dict[str, Any]:
    result = _create_text_ad_for_adgroup({
        "api_key": api_key,
        "secret_key": secret_key,
        "customer_id": cid,
    }, adgroup_id, headline, description, pc_url, mobile_url)
    if result.get("ok"):
        return _result_item(row_no, True, headline or adgroup_id, "생성 완료")
    return _result_item(row_no, False, headline or adgroup_id, result.get("detail") or "소재 생성 실패")
def bulk_upload_text_ads():
    api_key = str(request.form.get("api_key") or "").strip()
    secret_key = str(request.form.get("secret_key") or "").strip()
    cid = str(request.form.get("customer_id") or "").strip()
    upload = request.files.get("file")
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if upload is None or not getattr(upload, "filename", ""):
        return jsonify({"error": "업로드할 파일을 선택해주세요."}), 400
    try:
        rows = _read_uploaded_table(upload)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"업로드 파일 처리 실패: {e}"}), 400
    if not rows:
        return jsonify({"error": "파일에서 등록할 데이터가 없습니다."}), 400
    prepared: List[Tuple[int, str, str, str, str, str]] = []
    precheck_results: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        try:
            payload = _prepare_payload_row(row, "ad", cid)
        except Exception as e:
            precheck_results.append(_result_item(idx, False, f"{idx}행", f"행 파싱 실패: {e}"))
            continue
        adgroup_id = str(payload.get("nccAdgroupId") or "").strip()
        ad_obj = payload.get("ad") if isinstance(payload.get("ad"), dict) else {}
        raw_headline = str((ad_obj or {}).get("headline") or payload.get("headline") or "").strip()
        raw_description = str((ad_obj or {}).get("description") or payload.get("description") or "").strip()
        pc_url = _ensure_final_url((ad_obj.get("pc") or {}).get("final") if isinstance(ad_obj, dict) else payload.get("pcFinalUrl"))
        mobile_url = _ensure_final_url((ad_obj.get("mobile") or {}).get("final") if isinstance(ad_obj, dict) else payload.get("mobileFinalUrl")) or pc_url
        replace_keyword = str(row.get("replace_keyword") or row.get("대체키워드") or row.get("replaceKeyword") or "").strip()
        if not adgroup_id or not raw_headline or not raw_description or not pc_url:
            precheck_results.append(_result_item(idx, False, raw_headline or f"{idx}행", "그룹ID, 소재제목, 설명, PC 연결 URL은 필수입니다."))
            continue
        try:
            headline = _apply_keyword_insertion_template(raw_headline, replace_keyword)
            description = _apply_keyword_insertion_template(raw_description, replace_keyword)
        except ValueError as e:
            precheck_results.append(_result_item(idx, False, raw_headline or f"{idx}행", str(e)))
            continue
        headline_insert_cnt = _count_keyword_insertions(headline)
        desc_insert_cnt = _count_keyword_insertions(description)
        if headline_insert_cnt > 1:
            precheck_results.append(_result_item(idx, False, raw_headline or f"{idx}행", "키워드삽입은 제목에 1회까지만 사용할 수 있습니다."))
            continue
        if desc_insert_cnt > 2:
            precheck_results.append(_result_item(idx, False, raw_headline or f"{idx}행", "키워드삽입은 설명에 2회까지만 사용할 수 있습니다."))
            continue
        length_errors = _text_ad_length_errors(headline, description)
        if length_errors:
            precheck_results.append(_result_item(idx, False, raw_headline or f"{idx}행", " / ".join(length_errors)))
            continue
        prepared.append((idx, adgroup_id, headline, description, pc_url, mobile_url))
    exec_results: List[Dict[str, Any]] = []
    if prepared:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(_bulk_upload_one_text_ad, api_key, secret_key, cid, idx, adgroup_id, headline, description, pc_url, mobile_url)
                for idx, adgroup_id, headline, description, pc_url, mobile_url in prepared
            ]
            for future in as_completed(futures):
                exec_results.append(future.result())
    results = sorted(precheck_results + exec_results, key=lambda x: x.get("row_no", 0))
    success = sum(1 for item in results if item.get("ok"))
    fail = len(results) - success
    return jsonify({
        "ok": True,
        "total": len(results),
        "success": success,
        "fail": fail,
        "results": results,
        "message": f"소재 대량등록 완료 · 성공 {success}건 / 실패 {fail}건",
    })
def create_text_ad_simple():
    d = request.json or {}
    adgroup_ids = _parse_target_ids(d, "adgroup_id", "adgroup_ids")
    raw_headline = str(d.get("headline") or d.get("headline_template") or "").strip()
    raw_description = str(d.get("description") or d.get("description_template") or "").strip()
    replace_keyword = str(d.get("replace_keyword") or "").strip()
    pc_url = str(d.get("pc_url") or "").strip()
    mobile_url = str(d.get("mobile_url") or "").strip() or pc_url
    if not adgroup_ids or not raw_headline or not raw_description or not pc_url or not mobile_url:
        return jsonify({"error": "광고그룹, 제목, 설명, PC URL, 모바일 URL은 필수입니다."}), 400
    try:
        headline = _apply_keyword_insertion_template(raw_headline, replace_keyword)
        description = _apply_keyword_insertion_template(raw_description, replace_keyword)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    headline_insert_cnt = _count_keyword_insertions(headline)
    desc_insert_cnt = _count_keyword_insertions(description)
    if headline_insert_cnt > 1:
        return jsonify({"error": "키워드삽입은 제목에 1회까지만 사용할 수 있습니다."}), 400
    if desc_insert_cnt > 2:
        return jsonify({"error": "키워드삽입은 설명에 2회까지만 사용할 수 있습니다."}), 400
    length_errors = _text_ad_length_errors(headline, description)
    if length_errors:
        return jsonify({"error": " / ".join(length_errors), "headline": headline, "description": description}), 400
    results = [_create_text_ad_for_adgroup(d, adg_id, headline, description, pc_url, mobile_url) for adg_id in adgroup_ids]
    success = sum(1 for r in results if r.get("ok"))
    fail = len(results) - success
    if len(adgroup_ids) == 1:
        r = results[0]
        if r.get("ok"):
            return jsonify({"ok": True, "item": r.get("item"), "message": "기본소재 생성 완료"})
        payload = {
            "customerId": int(d.get("customer_id")),
            "nccAdgroupId": adgroup_ids[0],
            "type": "TEXT_45",
            "userLock": bool(d.get("user_lock", False)),
            "ad": {
                "headline": headline,
                "description": description,
                "pc": {"final": pc_url, "display": _display_url_from_final(pc_url)},
                "mobile": {"final": mobile_url, "display": _display_url_from_final(mobile_url)},
            },
        }
        return jsonify({"error": "소재 생성 실패", "details": r.get("detail"), "payload": payload}), 400
    return jsonify({
        "ok": True,
        "total": len(results),
        "success": success,
        "fail": fail,
        "results": results,
        "message": f"기본소재 등록 완료 · 성공 {success}건 / 실패 {fail}건",
    })
def create_ad_advanced():
    d = request.json or {}
    adgroup_id = str(d.get("adgroup_id") or "").strip()
    raw_json = str(d.get("raw_json") or "").strip()
    if not adgroup_id or not raw_json:
        return jsonify({"error": "광고그룹과 JSON 입력이 필요합니다."}), 400
    try:
        payload = json.loads(raw_json)
    except Exception as e:
        return jsonify({"error": f"JSON 파싱 실패: {e}"}), 400
    payload.setdefault("nccAdgroupId", adgroup_id)
    payload.setdefault("customerId", int(d.get("customer_id")))
    ad_type = str(payload.get("type") or "")
    if ad_type in SHOPPING_AD_TYPES:
        res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ads", params={"nccAdgroupId": adgroup_id, "isList": "true"}, json_body=[payload])
    else:
        res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ads", params={"nccAdgroupId": adgroup_id}, json_body=payload)
    if res.status_code in [200, 201]:
        return jsonify({"ok": True, "item": res.json(), "message": "고급 소재 생성 완료"})
    return jsonify({"error": "고급 소재 생성 실패", "details": res.text, "payload": payload}), 400
def create_extension_simple():
    d = request.json or {}
    owner_ids = _parse_target_ids(d, "owner_id", "owner_ids")
    adgroup_ids = _parse_target_ids(d, "adgroup_id", "adgroup_ids")
    campaign_ids = _parse_target_ids(d, "campaign_id", "campaign_ids")
    ext_type = _normalize_extension_type(d.get("type"))
    if not ext_type:
        return jsonify({"error": "확장소재 유형은 필수입니다."}), 400
    if ext_type not in {"HEADLINE", "DESCRIPTION_EXTRA", "DESCRIPTION", "SUB_LINKS", "PROMOTION", "SHOPPING_EXTRA"}:
        return jsonify({"error": "현재 간편 등록은 추가제목 / 홍보문구 / 추가설명 / 서브링크 / 쇼핑 추가홍보문구 / 쇼핑상품부가정보만 지원합니다."}), 400
    data: Dict[str, Any] = {}
    position = _parse_extension_position(d.get("position"))
    warnings: List[str] = []
    if ext_type == "HEADLINE":
        headline = str(d.get("headline") or "").strip()
        if not headline or len(headline) > 15:
            return jsonify({"error": "추가제목은 1~15자로 입력해야 합니다."}), 400
        data["headline"] = headline
    elif ext_type == "DESCRIPTION_EXTRA":
        description = str(d.get("description") or "").strip()
        if not description or len(description) > 14:
            return jsonify({"error": "홍보문구는 1~14자로 입력해야 합니다."}), 400
        data["description"] = description
    elif ext_type == "PROMOTION":
        basic_text = str(d.get("basic_text") or "").strip()
        additional_text = str(d.get("additional_text") or "").strip()
        if not basic_text:
            return jsonify({"error": "문구 1은 필수입니다."}), 400
        if len(basic_text) > 10:
            return jsonify({"error": "문구 1은 10자 이내로 입력해야 합니다."}), 400
        if additional_text and len(additional_text) > 30:
            return jsonify({"error": "문구 2는 30자 이내로 입력해야 합니다."}), 400
        data["basicText"] = basic_text
        if additional_text:
            data["additionalText"] = additional_text
    elif ext_type == "DESCRIPTION":
        description = str(d.get("description") or "").strip()
        if not description:
            return jsonify({"error": "추가설명은 필수입니다."}), 400
        data["description"] = description
    elif ext_type == "SUB_LINKS":
        raw_links = d.get("links") or []
        links = []
        for item in raw_links:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            final = str(item.get("final") or "").strip()
            if not name and not final:
                continue
            if len(name) > 6:
                return jsonify({"error": f"서브링크명 '{name}' 은(는) 6자 이내여야 합니다."}), 400
            if not re.match(r"^https?://", final, re.I):
                return jsonify({"error": f"서브링크 URL은 http:// 또는 https:// 로 시작해야 합니다. ({final})"}), 400
            links.append({"name": name, "final": final})
        if len(links) < 3 or len(links) > 4:
            return jsonify({"error": "서브링크는 최소 3개, 최대 4개까지 등록할 수 있습니다."}), 400
        data["links"] = links
    elif ext_type == "SHOPPING_EXTRA":
        resolved_owner_ids, warnings = _resolve_shopping_extra_owner_ids(
            d.get("api_key"), d.get("secret_key"), d.get("customer_id"),
            campaign_ids=campaign_ids, adgroup_ids=(adgroup_ids or [])
        )
        if resolved_owner_ids:
            owner_ids = resolved_owner_ids
        elif not owner_ids:
            return jsonify({
                "error": "쇼핑상품부가정보를 추가할 쇼핑 상품소재를 찾지 못했습니다.",
                "warnings": warnings,
            }), 400
    if not owner_ids:
        return jsonify({"error": "대상 광고그룹 또는 캠페인을 선택해주세요."}), 400
    results = []
    success = fail = 0
    for owner_id in owner_ids:
        payload = _build_extension_payload(owner_id, ext_type, data, int(d.get("customer_id")), position=position)
        if ext_type != "SHOPPING_EXTRA" and not payload.get("adExtension"):
            results.append({"ok": False, "owner_id": owner_id, "detail": "확장소재 내용이 비어 있습니다."})
            fail += 1
            continue
        existing = _find_existing_extension(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), owner_id, ext_type, data)
        if existing:
            detail_msg = "이미 동일한 확장소재가 등록되어 있습니다."
            if ext_type == "HEADLINE" and position in {1, 2}:
                upd = _apply_headline_position_best_effort(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), existing, position)
                if upd.get("ok"):
                    existing = upd.get("item") or existing
                    verify = _verify_headline_pin(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), owner_id, data.get("headline"), position)
                    if verify.get("ok"):
                        existing = verify.get("item") or existing
                        detail_msg = f"이미 등록된 추가제목의 노출 위치를 {position}번으로 반영했습니다. · {verify.get('detail')}"
                    else:
                        warnings.append(verify.get('detail'))
                        detail_msg = f"이미 등록된 추가제목의 노출 위치를 {position}번으로 반영 요청했습니다."
                else:
                    warnings.append(f"추가제목 위치 {position}번 반영 미확인: {upd.get('detail')}")
            success += 1
            results.append({"ok": True, "owner_id": owner_id, "detail": detail_msg, "item": existing, "exists": True})
            continue
        if ext_type == "SHOPPING_EXTRA":
            ok_created, res, used_payload, attempts = _create_shopping_extra_with_fallbacks(
                d.get("api_key"), d.get("secret_key"), d.get("customer_id"), owner_id
            )
            if ok_created and res is not None:
                success += 1
                item = None
                try:
                    item = res.json()
                except Exception:
                    item = None
                results.append({"ok": True, "owner_id": owner_id, "detail": "등록 완료", "item": item, "payload": used_payload})
                continue
            payload = used_payload or payload
        else:
            res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
            attempts = []
        if res.status_code in [200, 201]:
            success += 1
            item = None
            try:
                item = res.json()
            except Exception:
                item = None
            item_one = _extract_extension_item(item)
            detail_msg = "등록 완료"
            if ext_type == "HEADLINE" and position in {1, 2} and item_one:
                upd = _apply_headline_position_best_effort(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), item_one, position)
                if upd.get("ok"):
                    verify = _verify_headline_pin(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), owner_id, data.get("headline"), position)
                    if verify.get("ok"):
                        item = verify.get("item") or upd.get("item") or item
                        detail_msg = f"등록 완료 (위치 {position} 반영 · {verify.get('detail')})"
                    else:
                        item = upd.get("item") or item
                        warnings.append(verify.get('detail'))
                        detail_msg = f"등록 완료 (위치 {position} 반영 요청)"
                else:
                    warnings.append(f"추가제목 위치 {position}번 반영 미확인: {upd.get('detail')}")
            results.append({"ok": True, "owner_id": owner_id, "detail": detail_msg, "item": item})
        else:
            if res.status_code == 400 and "A record with the same name already exists." in (res.text or ""):
                existing = _find_existing_extension(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), owner_id, ext_type, data)
                if existing:
                    success += 1
                    results.append({"ok": True, "owner_id": owner_id, "detail": "이미 동일한 확장소재가 등록되어 있습니다.", "item": existing, "exists": True})
                    continue
            fail += 1
            extra_detail = res.text
            if ext_type == "SHOPPING_EXTRA" and attempts:
                extra_detail = f"{res.text}\n\n시도한 payload 후보 {len(attempts)}건:\n" + "\n".join(
                    f"{a['try']}. status={a['status']} detail={a['detail']}" for a in attempts[:8]
                )
            results.append({"ok": False, "owner_id": owner_id, "detail": extra_detail, "payload": payload})
    if len(owner_ids) == 1:
        r = results[0]
        if r.get("ok"):
            out = {"ok": True, "item": r.get("item"), "message": "확장소재 등록 완료"}
            if warnings:
                out["warnings"] = warnings
            return jsonify(out)
        return jsonify({"error": "확장소재 등록 실패", "details": r.get("detail"), "payload": r.get("payload"), "warnings": warnings}), 400
    out = {
        "ok": fail == 0,
        "total": len(owner_ids),
        "success": success,
        "fail": fail,
        "results": results,
        "message": f"확장소재 등록 완료 (성공 {success}건 / 실패 {fail}건)"
    }
    if warnings:
        out["warnings"] = warnings
    return jsonify(out)
def bulk_upload_headlines():
    api_key = str(request.form.get("api_key") or "").strip()
    secret_key = str(request.form.get("secret_key") or "").strip()
    cid = str(request.form.get("customer_id") or "").strip()
    upload = request.files.get("file")
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if upload is None or not getattr(upload, "filename", ""):
        return jsonify({"error": "업로드할 파일을 선택해주세요."}), 400
    try:
        rows = _read_uploaded_table(upload)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"업로드 파일 처리 실패: {e}"}), 400
    if not rows:
        return jsonify({"error": "파일에서 등록할 데이터가 없습니다."}), 400
    owner_keys = ["ownerId", "owner_id", "ownerid", "nccAdgroupId", "adgroupId", "광고그룹id", "광고그룹아이디", "그룹id", "그룹아이디"]
    headline_keys = ["headline", "title", "name", "추가제목", "제목"]
    pin_keys = ["pin", "position", "노출위치", "노출가능위치", "노출가능위치지정", "위치", "위치지정"]
    results: List[Dict[str, Any]] = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        owner_id = str(_row_pick_value(row, owner_keys) or "").strip()
        headline = str(_row_pick_value(row, headline_keys) or "").strip()
        raw_pin = _row_pick_value(row, pin_keys)
        has_pin_field = _row_has_any_key(row, pin_keys)
        if not owner_id or not headline:
            fail += 1
            results.append(_result_item(idx, False, headline or f"{idx}행", "그룹ID/광고그룹ID와 추가제목은 필수입니다."))
            continue
        if len(headline) > 15:
            fail += 1
            results.append(_result_item(idx, False, headline, "추가제목은 15자 이내로 입력해야 합니다."))
            continue
        try:
            position = _normalize_headline_pin_input(raw_pin)
        except ValueError as e:
            fail += 1
            results.append(_result_item(idx, False, headline, str(e)))
            continue
        existing = _find_existing_extension(api_key, secret_key, cid, owner_id, "HEADLINE", {"headline": headline})
        if existing:
            if has_pin_field:
                upd = _apply_headline_pin_best_effort(api_key, secret_key, cid, existing, position)
                if not upd.get("ok"):
                    fail += 1
                    results.append(_result_item(idx, False, headline, upd.get("detail") or "노출위치 반영 실패"))
                    continue
            verify = _verify_headline_pin(api_key, secret_key, cid, owner_id, headline, position)
            if verify.get("ok"):
                success += 1
                results.append(_result_item(idx, True, headline, f"이미 등록됨 · {verify.get('detail')}"))
            else:
                fail += 1
                results.append(_result_item(idx, False, headline, f"이미 등록됨 · {verify.get('detail')}"))
            continue
        payload = _build_extension_payload(owner_id, "HEADLINE", {"headline": headline}, int(cid), position=position)
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
        if res.status_code not in [200, 201]:
            fail += 1
            results.append(_result_item(idx, False, headline, res.text))
            continue
        created_item = None
        try:
            created_item = _extract_extension_item(res.json())
        except Exception:
            created_item = None
        if has_pin_field:
            target_item = created_item or _find_existing_extension(api_key, secret_key, cid, owner_id, "HEADLINE", {"headline": headline})
            if isinstance(target_item, dict):
                upd = _apply_headline_pin_best_effort(api_key, secret_key, cid, target_item, position)
                if not upd.get("ok"):
                    fail += 1
                    results.append(_result_item(idx, False, headline, f"등록은 되었으나 노출위치 반영 실패: {upd.get('detail')}"))
                    continue
        verify = _verify_headline_pin(api_key, secret_key, cid, owner_id, headline, position)
        if verify.get("ok"):
            success += 1
            results.append(_result_item(idx, True, headline, f"등록 완료 · {verify.get('detail')}"))
        else:
            fail += 1
            results.append(_result_item(idx, False, headline, f"등록은 되었으나 {verify.get('detail')}"))
    return jsonify({
        "ok": fail == 0,
        "total": len(rows),
        "success": success,
        "fail": fail,
        "results": results,
        "message": f"추가제목 대량등록 완료 (성공 {success}건 / 실패 {fail}건)",
    })
def create_shopping_ad_simple():
    d = request.json or {}
    adgroup_ids = _parse_target_ids(d, "adgroup_id", "adgroup_ids")
    reference_key = str(d.get("reference_key") or "").strip()
    ad_type = str(d.get("ad_type") or "SHOPPING_PRODUCT_AD").strip() or "SHOPPING_PRODUCT_AD"
    if not adgroup_ids or not reference_key:
        return jsonify({"error": "광고그룹과 상품번호(referenceKey)는 필수입니다."}), 400
    results = [_create_shopping_ad_for_adgroup(d, adg_id, reference_key, ad_type) for adg_id in adgroup_ids]
    success = sum(1 for r in results if r.get("ok"))
    fail = len(results) - success
    if len(adgroup_ids) == 1:
        r = results[0]
        if r.get("ok"):
            return jsonify({"ok": True, "item": r.get("item"), "message": "쇼핑 소재 등록 완료"})
        payload = {
            "customerId": int(d.get("customer_id")),
            "nccAdgroupId": adgroup_ids[0],
            "type": ad_type,
            "referenceKey": reference_key,
            "useGroupBidAmt": bool(d.get("use_group_bid_amt", True)),
            "bidAmt": int(d.get("bid_amt") or 0),
            "userLock": bool(d.get("user_lock", False)),
            "ad": {},
        }
        return jsonify({"error": "쇼핑 소재 등록 실패", "details": r.get("detail"), "payload": payload}), 400
    return jsonify({
        "ok": True,
        "total": len(results),
        "success": success,
        "fail": fail,
        "results": results,
        "message": f"쇼핑 소재 등록 완료 · 성공 {success}건 / 실패 {fail}건",
    })
def create_extension_raw():
    d = request.json or {}
    owner_id = str(d.get("owner_id") or "").strip()
    raw_json = str(d.get("raw_json") or "").strip()
    if not owner_id or not raw_json:
        return jsonify({"error": "광고그룹과 JSON 입력이 필요합니다."}), 400
    try:
        payload = json.loads(raw_json)
    except Exception as e:
        return jsonify({"error": f"JSON 파싱 실패: {e}"}), 400
    payload.setdefault("ownerId", owner_id)
    payload.setdefault("customerId", int(d.get("customer_id")))
    ext_type = _normalize_extension_type(payload.get("type"))
    payload["type"] = ext_type
    res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
    if res.status_code in [200, 201]:
        return jsonify({"ok": True, "item": res.json(), "message": "확장소재 생성 완료"})
    return jsonify({"error": "확장소재 생성 실패", "details": res.text, "payload": payload}), 400
def create_restricted_keywords_simple():
    d = request.json or {}
    raw_adgroup_ids = d.get("adgroup_ids") or []
    adgroup_ids: List[str] = []
    if isinstance(raw_adgroup_ids, list):
        for x in raw_adgroup_ids:
            sx = str(x or "").strip()
            if sx and sx not in adgroup_ids:
                adgroup_ids.append(sx)
    adgroup_id = str(d.get("adgroup_id") or "").strip()
    if adgroup_id and adgroup_id not in adgroup_ids:
        adgroup_ids.append(adgroup_id)
    keywords = [x.strip() for x in str(d.get("keywords") or "").replace(",", "\n").splitlines() if x.strip()]
    if not adgroup_ids or not keywords:
        return jsonify({"error": "광고그룹과 제외키워드는 필수입니다."}), 400
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    customer_id = d.get("customer_id")
    requested_type = str(d.get("match_type") or "EXACT").strip()
    new_rows = []
    skipped = []
    lookup_fail = []
    for adgroup_id in adgroup_ids:
        res_existing, existing_rows = _fetch_restricted_keywords(api_key, secret_key, customer_id, adgroup_id)
        existing_set = set()
        if res_existing.status_code == 200:
            for item in existing_rows:
                if isinstance(item, dict):
                    kw = str(item.get("keyword") or item.get("restrictedKeyword") or "").strip().lower()
                    if kw:
                        existing_set.add(kw)
                elif isinstance(item, str):
                    kw = item.strip().lower()
                    if kw:
                        existing_set.add(kw)
        else:
            lookup_fail.append(adgroup_id)
        for kw in keywords:
            if kw.lower() in existing_set:
                skipped.append({"adgroup_id": adgroup_id, "keyword": kw})
                continue
            new_rows.append({"nccAdgroupId": adgroup_id, "keyword": kw, "type": requested_type})
    success, fail, results = _bulk_create_restricted_keywords(api_key, secret_key, customer_id, new_rows)
    return jsonify({
        "ok": True,
        "total_adgroups": len(adgroup_ids),
        "total_keywords": len(keywords),
        "submitted": len(new_rows),
        "skipped_duplicates": skipped,
        "lookup_fail_adgroups": lookup_fail,
        "success": success,
        "fail": fail,
        "results": results,
    })
def copy_entities_to_adgroups():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    source_ids = _unique_keep_order(d.get("source_ids") or [])
    target_adgroup_ids = _unique_keep_order(d.get("target_adgroup_ids") or [])
    include_keywords = _boolish(d.get("include_keywords"), True)
    include_ads = _boolish(d.get("include_ads"), True)
    include_extensions = _boolish(d.get("include_extensions"), True)
    include_negatives = _boolish(d.get("include_negatives"), True)
    if not source_ids:
        return jsonify({"error": "복사 원본 광고그룹을 선택해주세요."}), 400
    if not target_adgroup_ids:
        return jsonify({"error": "대상 광고그룹을 선택해주세요."}), 400
    success = 0
    fail = 0
    skipped_same = 0
    all_errors: List[str] = []
    for src_id in source_ids:
        for target_id in target_adgroup_ids:
            if str(src_id) == str(target_id):
                skipped_same += 1
                continue
            errs, summary = _copy_adgroup_children(
                api_key, secret_key, cid,
                str(src_id), str(target_id), None,
                include_keywords=include_keywords,
                include_ads=include_ads,
                include_extensions=include_extensions,
                include_negatives=include_negatives,
                return_summary=True,
            )
            summary_line = _format_copy_summary(summary)
            if errs:
                fail += 1
                all_errors.append(f"[원본 {src_id} → 대상 {target_id}] {summary_line}")
                all_errors.extend([f"[원본 {src_id} → 대상 {target_id}] {e}" for e in errs])
            else:
                success += 1
                all_errors.append(f"[원본 {src_id} → 대상 {target_id}] {summary_line}")
    msg = f"항목 복사 완료! (성공: {success}, 실패: {fail}, 동일 그룹 건너뜀: {skipped_same})"
    if all_errors:
        msg += "\n" + "\n".join(all_errors[:60])
    _cache_invalidate(d.get("api_key"), d.get("secret_key"), d.get("customer_id"))
    return jsonify({"ok": True, "message": msg})
def copy_campaigns():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    src_ids, suffix = d.get("source_ids", []), d.get("suffix", "_복사본")
    try:
        copy_count = int(d.get("copy_count") or 1)
    except (TypeError, ValueError):
        return jsonify({"error": "복사 개수는 1 이상의 숫자여야 합니다."}), 400
    if copy_count <= 0:
        return jsonify({"error": "복사 개수는 1 이상의 숫자여야 합니다."}), 400
    custom_names_raw = d.get("custom_names") if d.get("custom_names") is not None else d.get("custom_names_text")
    results, all_errors = {"success": 0, "fail": 0}, []
    source_name_map: Dict[str, str] = {}
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/campaigns/{src_id}")
        if r_get.status_code != 200:
            results["fail"] += 1
            all_errors.append(f"[원본 캠페인 {src_id}] 조회 실패: {r_get.text}")
            continue
        src = r_get.json() or {}
        source_name_map[str(src_id)] = str(src.get("name") or "").strip()
    valid_src_ids = [str(src_id) for src_id in src_ids if str(src_id) in source_name_map]
    if valid_src_ids:
        try:
            planned_names = _resolve_copy_names([source_name_map[x] for x in valid_src_ids], copy_count, suffix, custom_names_raw)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        camp_res, camp_rows = _fetch_campaigns(api_key, secret_key, cid)
        if camp_res.status_code != 200:
            return jsonify({"error": f"기존 캠페인 이름 조회 실패: {camp_res.text}"}), 400
        name_err = _validate_name_candidates(planned_names, [row.get("name") for row in (camp_rows or [])], "캠페인")
        if name_err:
            return jsonify({"error": name_err}), 400
        planned_iter = iter(planned_names)
    else:
        planned_iter = iter([])
    for src_id in src_ids:
        if str(src_id) not in source_name_map:
            continue
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/campaigns/{src_id}")
        if r_get.status_code != 200:
            continue
        src = r_get.json() or {}
        source_name = str(src.get("name") or "").strip()
        r_adgs, src_adgroups = _fetch_adgroups(api_key, secret_key, cid, str(src_id))
        if r_adgs.status_code != 200:
            results["fail"] += copy_count
            all_errors.append(f"[{source_name or src_id}] 원본 광고그룹 조회 실패: {r_adgs.text}")
            continue
        for idx in range(1, copy_count + 1):
            new_camp = {
                "customerId": int(cid),
                "name": next(planned_iter, _build_copy_name(source_name, idx, copy_count, suffix)),
                "campaignTp": src.get("campaignTp"),
                "useDailyBudget": src.get("useDailyBudget", False),
                "dailyBudget": src.get("dailyBudget", 0),
            }
            r_post = _do_req("POST", api_key, secret_key, cid, "/ncc/campaigns", json_body=new_camp)
            if r_post.status_code not in [200, 201]:
                results["fail"] += 1
                all_errors.append(f"[{new_camp['name']}] 생성 실패: {r_post.text}")
                continue
            results["success"] += 1
            new_campaign_id = str((r_post.json() or {}).get("nccCampaignId") or "").strip()
            if not new_campaign_id:
                all_errors.append(f"[{new_camp['name']}] 신규 캠페인 ID 확인 실패")
                continue
            for row in (src_adgroups or []):
                src_adg_id = str(row.get("id") or row.get("nccAdgroupId") or "").strip()
                if not src_adg_id:
                    continue
                r_adg = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{src_adg_id}")
                if r_adg.status_code != 200:
                    all_errors.append(f"[{new_camp['name']}] 원본 광고그룹 {src_adg_id} 조회 실패: {r_adg.text}")
                    continue
                src_adg = r_adg.json() or {}
                new_adg = _extract_adgroup(src_adg, new_campaign_id, cid, None)
                r_new_adg = _do_req("POST", api_key, secret_key, cid, "/ncc/adgroups", json_body=new_adg)
                if r_new_adg.status_code not in [200, 201]:
                    all_errors.append(f"[{new_camp['name']} > {src_adg.get('name')}] 광고그룹 생성 실패: {r_new_adg.text}")
                    continue
                new_adg_id = str((r_new_adg.json() or {}).get("nccAdgroupId") or "").strip()
                errs, summary = _copy_adgroup_children(
                    api_key, secret_key, cid,
                    src_adg_id, new_adg_id, None,
                    include_keywords=True,
                    include_ads=True,
                    include_extensions=True,
                    include_negatives=True,
                    return_summary=True,
                )
                all_errors.append(f"[{new_camp['name']} > {src_adg.get('name')}] {_format_copy_summary(summary)}")
                all_errors.extend([f"[{new_camp['name']} > {src_adg.get('name')}] {e}" for e in errs])
                _, media_msgs = _copy_adgroup_media_settings(api_key, secret_key, cid, src_adg_id, new_adg_id)
                if media_msgs:
                    for msg in media_msgs:
                        if msg and (("실패" in msg) or ("없음" in msg) or ("비어" in msg) or ("조회" in msg)):
                            all_errors.append(f"[{new_camp['name']} > {src_adg.get('name')}] 매체 설정: {msg}")
                ok_extra_targets, extra_target_msgs = _copy_adgroup_extra_target_settings(api_key, secret_key, cid, src_adg_id, new_adg_id)
                if extra_target_msgs:
                    for msg in extra_target_msgs:
                        if _should_surface_target_copy_message(msg):
                            all_errors.append(f"[{new_camp['name']} > {src_adg.get('name')}] 타겟 설정: {msg}")
                if not ok_extra_targets and not extra_target_msgs:
                    all_errors.append(f"[{new_camp['name']} > {src_adg.get('name')}] 타겟 설정: 원본 타겟 확인/복사 실패")
                ok_search_opts, search_opts_msg = _copy_adgroup_search_option_settings(api_key, secret_key, cid, src_adg_id, new_adg_id)
                if (not ok_search_opts) and search_opts_msg:
                    all_errors.append(f"[{new_camp['name']} > {src_adg.get('name')}] 검색옵션: {search_opts_msg}")
    msg = f"캠페인 복사 완료!\n(성공: {results['success']}개, 실패: {results['fail']}개)"
    if all_errors:
        msg += "\n" + "\n".join(all_errors[:60])
    _cache_invalidate(d.get("api_key"), d.get("secret_key"), d.get("customer_id"))
    return jsonify({"ok": True, "message": msg})
def copy_adgroups_to_target():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    source_ids = _unique_keep_order(d.get("source_ids") or [])
    source_campaign_ids = _unique_keep_order(d.get("source_campaign_ids") or [])
    src_ids, resolve_warnings = _resolve_copy_source_adgroup_ids(api_key, secret_key, cid, source_ids, source_campaign_ids)
    target_camp_id = d.get("target_campaign_id")
    suffix = d.get("suffix", "_복사본")
    try:
        copy_count = int(d.get("copy_count") or 1)
    except (TypeError, ValueError):
        return jsonify({"error": "복사 개수는 1 이상의 숫자여야 합니다."}), 400
    if copy_count <= 0:
        return jsonify({"error": "복사 개수는 1 이상의 숫자여야 합니다."}), 400
    custom_names_raw = d.get("custom_names") if d.get("custom_names") is not None else d.get("custom_names_text")
    biz_channel_id = d.get("biz_channel_id")
    include_keywords = _boolish(d.get("include_keywords"), True)
    include_ads = _boolish(d.get("include_ads"), True)
    include_extensions = _boolish(d.get("include_extensions"), True)
    include_negatives = _boolish(d.get("include_negatives"), True)
    copy_media = _boolish(d.get("copy_media"), True)
    copy_as_off = _boolish(d.get("copy_as_off"), False)
    if not src_ids:
        return jsonify({"error": "복사할 광고그룹 또는 캠페인을 선택해주세요."}), 400
    if not str(target_camp_id or "").strip():
        return jsonify({"error": "대상 캠페인을 선택해주세요."}), 400
    source_name_map: Dict[str, str] = {}
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{src_id}")
        if r_get.status_code != 200:
            continue
        src_obj = r_get.json() or {}
        source_name_map[str(src_id)] = str(src_obj.get("name") or "").strip()
    valid_src_ids = [str(src_id) for src_id in src_ids if str(src_id) in source_name_map]
    if valid_src_ids:
        try:
            planned_names = _resolve_copy_names([source_name_map[x] for x in valid_src_ids], copy_count, suffix, custom_names_raw)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        adg_res, existing_target_rows = _fetch_adgroups(api_key, secret_key, cid, str(target_camp_id), enrich_media=False)
        if adg_res.status_code != 200:
            return jsonify({"error": f"대상 캠페인 광고그룹 이름 조회 실패: {adg_res.text}"}), 400
        name_err = _validate_name_candidates(planned_names, [row.get("name") for row in (existing_target_rows or [])], "광고그룹")
        if name_err:
            return jsonify({"error": name_err}), 400
        planned_iter = iter(planned_names)
    else:
        planned_iter = iter([])
    results, all_errors = {"success": 0, "fail": 0}, list(resolve_warnings)
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{src_id}")
        if r_get.status_code != 200:
            results["fail"] += 1
            all_errors.append(f"[원본 {src_id}] 조회 실패: {r_get.text}")
            continue
        src_obj = r_get.json() or {}
        source_name = str(src_obj.get("name") or "").strip()
        for idx in range(1, copy_count + 1):
            new_adg = _extract_adgroup(src_obj, target_camp_id, cid, biz_channel_id)
            new_adg["name"] = next(planned_iter, _build_copy_name(source_name, idx, copy_count, suffix))
            r_post = _do_req("POST", api_key, secret_key, cid, "/ncc/adgroups", json_body=new_adg)
            if r_post.status_code in [200, 201]:
                results["success"] += 1
                new_adg_id = str((r_post.json() or {}).get("nccAdgroupId") or "").strip()
                errs, summary = _copy_adgroup_children(
                    api_key, secret_key, cid, src_id, new_adg_id, biz_channel_id,
                    include_keywords=include_keywords, include_ads=include_ads,
                    include_extensions=include_extensions, include_negatives=include_negatives,
                    return_summary=True,
                )
                all_errors.append(f"[{new_adg['name']}] {_format_copy_summary(summary)}")
                all_errors.extend([f"[{new_adg['name']}] {e}" for e in errs])
                if copy_media and new_adg_id:
                    ok_media, media_msgs = _copy_adgroup_media_settings(api_key, secret_key, cid, str(src_id), new_adg_id)
                    if media_msgs:
                        for msg in media_msgs:
                            if msg and (("실패" in msg) or ("없음" in msg) or ("비어" in msg) or ("조회" in msg)):
                                all_errors.append(f"[{new_adg['name']}] 매체 설정: {msg}")
                if new_adg_id:
                    ok_extra_targets, extra_target_msgs = _copy_adgroup_extra_target_settings(api_key, secret_key, cid, str(src_id), new_adg_id)
                    if extra_target_msgs:
                        for msg in extra_target_msgs:
                            if _should_surface_target_copy_message(msg):
                                all_errors.append(f"[{new_adg['name']}] 타겟 설정: {msg}")
                    if not ok_extra_targets and not extra_target_msgs:
                        all_errors.append(f"[{new_adg['name']}] 타겟 설정: 원본 타겟 확인/복사 실패")
                if new_adg_id:
                    ok_search_opts, search_opts_msg = _copy_adgroup_search_option_settings(api_key, secret_key, cid, str(src_id), new_adg_id)
                    if (not ok_search_opts) and search_opts_msg:
                        all_errors.append(f"[{new_adg['name']}] 검색옵션: {search_opts_msg}")
                if copy_as_off and new_adg_id:
                    ok_off, off_msg = _set_user_lock_for_entity(api_key, secret_key, cid, "adgroup", new_adg_id, False)
                    if not ok_off and off_msg:
                        all_errors.append(f"[{new_adg['name']}] OFF 설정 실패: {off_msg}")
            else:
                results["fail"] += 1
                all_errors.append(f"[{new_adg['name']}] 생성 실패: {r_post.text}")
    _cache_invalidate(api_key, secret_key, cid)
    return jsonify({"ok": True, "message": f"복사 완료! (성공: {results['success']}, 실패: {results['fail']})\n" + "\n".join(all_errors[:60])})
def rename_adgroups_bulk():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_ids = [str(x).strip() for x in (d.get("entity_ids") or []) if str(x).strip()]
    target_names = _parse_multiline_names(d.get("target_names") if d.get("target_names") is not None else d.get("target_names_text"))
    if not entity_ids:
        return jsonify({"error": "이름을 변경할 광고그룹을 선택해주세요."}), 400
    if not target_names:
        return jsonify({"error": "변경할 광고그룹명을 1개 이상 입력해주세요."}), 400
    if len(entity_ids) != len(target_names):
        return jsonify({"error": f"선택한 광고그룹 수({len(entity_ids)})와 입력한 이름 수({len(target_names)})가 다릅니다."}), 400
    detail_map: Dict[str, Dict[str, Any]] = {}
    campaign_to_selected: Dict[str, List[str]] = defaultdict(list)
    for adg_id in entity_ids:
        res_get, obj = _fetch_adgroup_detail(api_key, secret_key, cid, adg_id)
        if res_get.status_code != 200 or not obj:
            return jsonify({"error": f"광고그룹 조회 실패 ({adg_id}): {res_get.text}"}), 400
        detail_map[adg_id] = obj
        campaign_id = str(obj.get("nccCampaignId") or "").strip()
        campaign_to_selected[campaign_id].append(adg_id)
    rename_map = {entity_ids[idx]: target_names[idx] for idx in range(len(entity_ids))}
    for campaign_id, selected_ids in campaign_to_selected.items():
        adg_res, rows = _fetch_adgroups(api_key, secret_key, cid, campaign_id, enrich_media=False)
        if adg_res.status_code != 200:
            return jsonify({"error": f"캠페인 내 광고그룹 이름 조회 실패 ({campaign_id}): {adg_res.text}"}), 400
        existing_names = []
        selected_id_set = set(selected_ids)
        for row in (rows or []):
            row_id = str(row.get("id") or row.get("nccAdgroupId") or "").strip()
            if row_id in selected_id_set:
                continue
            existing_names.append(row.get("name"))
        planned_names = [rename_map[x] for x in selected_ids]
        name_err = _validate_name_candidates(planned_names, existing_names, "광고그룹")
        if name_err:
            return jsonify({"error": name_err}), 400
    success = fail = 0
    messages: List[str] = []
    for adg_id in entity_ids:
        obj = detail_map.get(adg_id) or {}
        new_name = rename_map.get(adg_id, "")
        old_name = str(obj.get("name") or adg_id)
        obj["name"] = new_name
        res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adg_id}", params={"fields": "name"}, json_body=obj)
        if res_put.status_code in [200, 201]:
            success += 1
            messages.append(f"[{old_name}] → [{new_name}] 변경 완료")
        else:
            fail += 1
            messages.append(f"[{old_name}] 이름 변경 실패: {res_put.text}")
    _cache_invalidate(api_key, secret_key, cid)
    return jsonify({
        "ok": success > 0,
        "message": f"광고그룹명 일괄 변경 완료! (성공: {success}, 실패: {fail})" + ("\n" + "\n".join(messages[:10]) if messages else ""),
        "success": success,
        "fail": fail,
    }), (200 if success > 0 else 400)
def update_media():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_ids = [str(x).strip() for x in (d.get("entity_ids") or []) if str(x).strip()]
    media_type = str(d.get("media_type") or "ALL").strip().upper()
    media_detail = _normalize_media_detail(d.get("media_detail"))
    if not entity_ids:
        return jsonify({"error": "선택된 광고그룹이 없습니다."}), 400
    results = []
    success = fail = 0
    for eid in entity_ids:
        row_ok = True
        row_msgs: List[str] = []
        ok_pm, detail_pm = _update_pc_mobile_target(api_key, secret_key, cid, eid, media_type)
        row_ok = row_ok and ok_pm
        row_msgs.append(f"PC/모바일: {detail_pm}")
        ok_media, detail_media = _update_media_target(api_key, secret_key, cid, eid, media_detail)
        row_ok = row_ok and ok_media
        row_msgs.append(f"세부매체: {detail_media}")
        results.append({"nccAdgroupId": eid, "ok": row_ok, "detail": " | ".join(row_msgs)})
        if row_ok:
            success += 1
        else:
            fail += 1
    status_code = 200 if success > 0 else 400
    return jsonify({"ok": success > 0, "message": f"총 {len(entity_ids)}개 매체 변경 성공: {success}개 / 실패: {fail}개", "success": success, "fail": fail, "results": results}), status_code
def _handle_update_adgroup_options_request():
    d = request.get_json(silent=True) or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    raw_entity_ids = [str(x).strip() for x in (d.get("entity_ids") or []) if str(x).strip()]
    entity_type = str(d.get("entity_type") or d.get("scope") or "adgroup").strip().lower()
    if entity_type not in {"campaign", "adgroup"}:
        return jsonify({"error": "entity_type은 campaign 또는 adgroup 이어야 합니다."}), 400
    media_type = d.get("media_type")
    media_detail = _normalize_media_detail(d.get("media_detail"))
    use_keyword_plus = d.get("use_keyword_plus")
    use_close_variant = d.get("use_close_variant")
    keyword_plus_weight = d.get("keyword_plus_weight")
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not raw_entity_ids:
        return jsonify({"error": "선택된 캠페인 또는 광고그룹이 없습니다."}), 400

    if entity_type == "campaign":
        entity_ids, resolve_warnings = _resolve_bulk_target_adgroup_ids(api_key, secret_key, cid, "campaign", raw_entity_ids, [])
    else:
        entity_ids, resolve_warnings = _resolve_bulk_target_adgroup_ids(api_key, secret_key, cid, "adgroup", [], raw_entity_ids)
    entity_ids = _unique_keep_order([str(x).strip() for x in (entity_ids or []) if str(x).strip()])
    if not entity_ids:
        msg = "적용할 광고그룹이 없습니다."
        if resolve_warnings:
            msg += "\n" + "\n".join(resolve_warnings[:10])
        return jsonify({"error": msg, "ok": False, "patch_version": PATCH_VERSION}), 400

    results = []
    success = fail = skipped = unchanged = 0
    for warn in (resolve_warnings or []):
        results.append({"nccAdgroupId": "", "name": "확인", "ok": None, "detail": warn})

    for adg_id in entity_ids:
        detail_res, adgroup_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adg_id)
        if detail_res.status_code != 200 or not adgroup_obj:
            fail += 1
            results.append({"nccAdgroupId": adg_id, "ok": False, "detail": f"광고그룹 조회 실패: {detail_res.text}"})
            continue
        name = str(adgroup_obj.get("name") or adg_id)
        row_msgs: List[str] = []
        row_ok = True
        row_changed = False

        if str(media_type or "").strip() != "" or d.get("media_detail") is not None:
            ok_media_pm, detail_media_pm = _update_pc_mobile_target(api_key, secret_key, cid, adg_id, media_type or "ALL")
            row_msgs.append(f"PC/모바일 매체: {detail_media_pm}")
            row_ok = row_ok and ok_media_pm
            row_changed = row_changed or bool(ok_media_pm)
            ok_media_network, detail_media_network = _update_media_target(api_key, secret_key, cid, adg_id, media_detail)
            row_msgs.append(f"세부 매체: {detail_media_network}")
            row_ok = row_ok and ok_media_network
            row_changed = row_changed or bool(ok_media_network)

        is_web_site = str(adgroup_obj.get("adgroupType") or "").upper() == "WEB_SITE"
        wants_search_options = (use_keyword_plus is not None) or (use_close_variant is not None) or (str(keyword_plus_weight or "").strip() != "")
        if wants_search_options:
            if not is_web_site:
                skipped += 1
                row_msgs.append("파워링크 그룹이 아니어서 확장검색/일치검색은 건너뜀")
            else:
                kwp = None
                if str(keyword_plus_weight or "").strip() != "":
                    try:
                        kwp = int(float(keyword_plus_weight))
                    except Exception:
                        row_ok = False
                        kwp = None
                        row_msgs.append("검색옵션: 확장검색 예산비율은 숫자로 입력해주세요.")
                if row_ok:
                    ok_opts, detail_opts = _update_adgroup_search_options(
                        api_key, secret_key, cid, adg_id,
                        use_keyword_plus=None if use_keyword_plus is None else _normalize_bool_for_api(use_keyword_plus, False),
                        keyword_plus_weight=kwp,
                        use_close_variant=None if use_close_variant is None else _normalize_bool_for_api(use_close_variant, False),
                    )
                    row_msgs.append(f"검색옵션: {detail_opts}")
                    row_ok = row_ok and ok_opts
                    if ok_opts and not str(detail_opts or "").startswith("이미 요청값과 동일"):
                        row_changed = True

        if row_ok:
            if row_changed:
                success += 1
            else:
                unchanged += 1
        else:
            fail += 1
        results.append({"nccAdgroupId": adg_id, "name": name, "ok": row_ok, "detail": " | ".join(row_msgs) if row_msgs else "변경 완료"})

    _cache_invalidate(api_key, secret_key, cid)
    tone_ok = success > 0 or unchanged > 0
    status_code = 200 if tone_ok else 400
    scope_label = "선택 캠페인 하위" if entity_type == "campaign" else "선택 광고그룹"
    message = f"{scope_label} 총 {len(entity_ids)}개 광고그룹 설정 변경 완료 · 변경 {success}개 / 유지 {unchanged}개 / 실패 {fail}개 / 건너뜀 {skipped}개"
    resp = {
        "ok": tone_ok,
        "message": message,
        "success": success,
        "unchanged": unchanged,
        "fail": fail,
        "skipped": skipped,
        "results": results,
        "patch_version": PATCH_VERSION,
    }
    if not tone_ok:
        resp["error"] = message + ("\n" + "\n".join([f"[{r.get('name') or r.get('nccAdgroupId')}] {r.get('detail')}" for r in results[:5]]) if results else "")
    return jsonify(resp), status_code

def update_adgroup_options():
    if request.method == "OPTIONS":
        return Response(status=204)
    if request.method == "GET":
        return jsonify({"ok": False, "error": "이 기능은 POST 요청으로만 실행됩니다. 화면의 버튼으로 다시 실행해주세요."}), 400
    return _handle_update_adgroup_options_request()


def update_powerlink_device_bid_weights():
    d = request.get_json(silent=True) or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    raw_pc_weight = d.get("pc_bid_weight")
    raw_mobile_weight = d.get("mobile_bid_weight")
    pc_weight = _normalize_optional_bid_weight(raw_pc_weight)
    mobile_weight = _normalize_optional_bid_weight(raw_mobile_weight)
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if str(raw_pc_weight or "").strip() and pc_weight is None:
        return jsonify({"error": "PC 입찰가중치는 10~500% 사이로 입력해주세요."}), 400
    if str(raw_mobile_weight or "").strip() and mobile_weight is None:
        return jsonify({"error": "모바일 입찰가중치는 10~500% 사이로 입력해주세요."}), 400
    if pc_weight is None and mobile_weight is None:
        return jsonify({"error": "변경할 PC 또는 모바일 입찰가중치를 1개 이상 입력해주세요."}), 400

    scope = str(d.get("entity_type") or d.get("scope") or "adgroup").strip().lower()
    entity_ids = [str(x).strip() for x in (d.get("entity_ids") or []) if str(x).strip()]
    if not entity_ids:
        return jsonify({"error": "좌측 체크박스에서 캠페인 또는 광고그룹을 선택해주세요."}), 400

    campaign_ids = entity_ids if scope == "campaign" else []
    adgroup_ids = entity_ids if scope != "campaign" else []
    target_adgroup_ids, warnings = _resolve_bulk_target_adgroup_ids(api_key, secret_key, cid, scope, campaign_ids, adgroup_ids)
    if not target_adgroup_ids:
        msg = "적용할 광고그룹이 없습니다."
        if warnings:
            msg += "\n" + "\n".join(warnings[:10])
        return jsonify({"error": msg}), 400

    success = 0
    unchanged = 0
    fail = 0
    skipped = 0
    details: List[str] = list(warnings)
    preferred_strategy: Optional[str] = None
    hard_stop = False

    for adg_id in target_adgroup_ids:
        ok, detail, obj, meta = _update_powerlink_device_bid_weight_for_adgroup(
            api_key, secret_key, cid, adg_id, pc_weight, mobile_weight, preferred_strategy=preferred_strategy
        )
        name = str((obj or {}).get("name") or adg_id)
        if ok is None:
            skipped += 1
            details.append(f"[{name}] 건너뜀: {detail}")
            continue
        if ok:
            strategy = str((meta or {}).get("strategy") or "")
            if strategy and strategy not in {"unchanged", "none"}:
                preferred_strategy = strategy
            if strategy == "unchanged" or str(detail or "").startswith("이미 동일값"):
                unchanged += 1
                details.append(f"[{name}] 유지: {detail}")
            else:
                success += 1
                details.append(f"[{name}] 변경 완료: {detail}")
            continue
        fail += 1
        details.append(f"[{name}] 변경 실패: {detail}")
        if (meta or {}).get("unsupported_field"):
            hard_stop = True
            break

    _cache_invalidate(api_key, secret_key, cid)
    total_done = success + unchanged + fail + skipped
    stopped_msg = " / 미지원 응답 감지로 나머지 그룹 적용 중단" if hard_stop and total_done < len(target_adgroup_ids) else ""
    tone_ok = success > 0 or unchanged > 0
    return jsonify({
        "ok": tone_ok,
        "success": success,
        "unchanged": unchanged,
        "fail": fail,
        "skipped": skipped,
        "message": f"파워링크 PC/모바일 입찰가중치 변경 완료 · 변경 {success}개 / 유지 {unchanged}개 / 실패 {fail}개 / 건너뜀 {skipped}개{stopped_msg}"
                   + ("\n" + "\n".join(details[:30]) if details else ""),
        "details": details,
    }), (200 if tone_ok else 400)

def apply_target_settings_bulk():
    d = request.get_json(silent=True) or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    source_adgroup_id = str(d.get("source_adgroup_id") or "").strip()

    raw_scopes = d.get("target_scopes")
    if isinstance(raw_scopes, list):
        target_scopes = [str(x or "").strip().lower() for x in raw_scopes if str(x or "").strip()]
    else:
        target_scopes = [str(d.get("target_scope") or "adgroup").strip().lower()]
    target_scopes = _unique_keep_order([x for x in target_scopes if x in {"campaign", "adgroup"}]) or ["adgroup"]

    # Backward compatibility: older UI only sent include_extra_targets=true.
    has_specific_profile_flags = any(k in d for k in [
        "include_age_targets", "include_gender_targets", "include_region_targets", "include_segment_targets"
    ])
    include_extra_targets = _boolish(d.get("include_extra_targets"), True)
    include_age = _boolish(d.get("include_age_targets"), include_extra_targets if not has_specific_profile_flags else False)
    include_gender = _boolish(d.get("include_gender_targets"), include_extra_targets if not has_specific_profile_flags else False)
    include_region = _boolish(d.get("include_region_targets"), include_extra_targets if not has_specific_profile_flags else False)
    include_segment = _boolish(d.get("include_segment_targets"), False)
    include_schedule = _boolish(d.get("include_schedule"), True)
    # 새 UI처럼 연령/성별/지역을 개별 선택한 경우에는 adgroupAttrJson 전체 복사를
    # 기본으로 하지 않는다. 전체 attr 복사는 선택 항목 외 설정까지 딸려갈 수 있다.
    include_attr_json = _boolish(d.get("include_attr_json"), include_extra_targets if not has_specific_profile_flags else False)

    campaign_ids = [str(x).strip() for x in (d.get("campaign_ids") or []) if str(x).strip()]
    adgroup_ids = [str(x).strip() for x in (d.get("adgroup_ids") or []) if str(x).strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not source_adgroup_id:
        return jsonify({"error": "원본 광고그룹을 선택해주세요."}), 400

    selected_criterion_types: List[str] = []
    if include_age:
        selected_criterion_types.extend(PROFILE_CRITERION_CATEGORY_TYPES["age"])
    if include_gender:
        selected_criterion_types.extend(PROFILE_CRITERION_CATEGORY_TYPES["gender"])
    if include_region:
        selected_criterion_types.extend(PROFILE_CRITERION_CATEGORY_TYPES["region"])
    if include_segment:
        selected_criterion_types.extend(PROFILE_CRITERION_CATEGORY_TYPES["segment"])
    selected_criterion_types = _normalize_profile_criterion_types(selected_criterion_types)

    if not selected_criterion_types and not include_schedule:
        return jsonify({"error": "적용할 타겟 항목을 1개 이상 선택해주세요."}), 400

    target_adgroup_ids: List[str] = []
    warnings: List[str] = []
    if "campaign" in target_scopes:
        resolved, warn = _resolve_bulk_target_adgroup_ids(api_key, secret_key, cid, "campaign", campaign_ids, [])
        target_adgroup_ids.extend(resolved)
        warnings.extend(warn)
    if "adgroup" in target_scopes:
        resolved, warn = _resolve_bulk_target_adgroup_ids(api_key, secret_key, cid, "adgroup", [], adgroup_ids)
        target_adgroup_ids.extend(resolved)
        warnings.extend(warn)
    target_adgroup_ids = [x for x in _unique_keep_order(target_adgroup_ids) if x and x != source_adgroup_id]
    if not target_adgroup_ids:
        msg = "적용할 대상 광고그룹이 없습니다."
        if warnings:
            msg += "\n" + "\n".join(warnings[:10])
        return jsonify({"error": msg}), 400

    source_schedule_ready = False
    source_schedule_msg = ""
    if include_schedule:
        res_schedule_src, schedule_rows = _fetch_schedule_entries(api_key, secret_key, cid, source_adgroup_id)
        if res_schedule_src.status_code == 200 and schedule_rows:
            source_schedule_ready = True
        elif res_schedule_src.status_code == 404:
            source_schedule_msg = "SCHEDULE 원본 설정 없음"
        elif res_schedule_src.status_code == 405:
            source_schedule_msg = f"SCHEDULE 원본 조회 미지원(405): {res_schedule_src.text}"
        else:
            source_schedule_msg = f"SCHEDULE 원본 조회 실패: {res_schedule_src.text}"

    item_labels: List[str] = []
    if include_age:
        item_labels.append("연령대")
    if include_gender:
        item_labels.append("성별")
    if include_region:
        item_labels.append("지역")
    if include_segment:
        item_labels.append("이용자 세그먼트")
    if include_schedule:
        item_labels.append("요일/시간대")

    success = 0
    fail = 0
    details: List[str] = list(warnings)
    for target_adgroup_id in target_adgroup_ids:
        row_msgs: List[str] = []
        hard_fail = False
        applied_any = False
        if selected_criterion_types:
            ok_extra, extra_msgs = _copy_adgroup_extra_targets_only(
                api_key, secret_key, cid, source_adgroup_id, target_adgroup_id,
                include_criterion_types=selected_criterion_types,
                include_target_rows=False,
                include_attr_json=include_attr_json,
            )
            row_msgs.extend([msg for msg in (extra_msgs or []) if msg])
            if ok_extra:
                applied_any = True
            else:
                if any("적용 실패" in str(m) or "복사 실패" in str(m) or "미반영" in str(m) for m in (extra_msgs or [])):
                    hard_fail = True
                elif not extra_msgs:
                    row_msgs.append("선택한 프로필 타겟 원본 없음")
        if include_schedule:
            if source_schedule_ready:
                ok_schedule, msg_schedule = _copy_schedule_criterion_exact(api_key, secret_key, cid, source_adgroup_id, target_adgroup_id)
                if ok_schedule:
                    applied_any = True
                elif msg_schedule:
                    row_msgs.append(msg_schedule)
                    hard_fail = True
            else:
                row_msgs.append(source_schedule_msg or "SCHEDULE 원본 설정 없음")
        if not applied_any:
            row_msgs.insert(0, "실제 반영된 항목 없음")
        row_ok = applied_any and not hard_fail
        if row_ok:
            success += 1
        else:
            fail += 1
        if row_msgs:
            details.append(f"[{target_adgroup_id}] " + " | ".join(_unique_keep_order(row_msgs)[:8]))
    status_code = 200 if success > 0 else 400
    scope_label = "+".join(["캠페인" if s == "campaign" else "광고그룹" for s in target_scopes])
    return jsonify({
        "ok": success > 0,
        "message": f"타겟 설정 일괄 적용 완료 · 범위 {scope_label} · 항목 {', '.join(item_labels) or '-'} · 성공 {success}개 / 실패 {fail}개"
                   + (("\n" + "\n".join(details[:20])) if details else ""),
        "success": success,
        "fail": fail,
        "details": details[:80],
        "patch": "bulk-target-source-select-v16-20260428",
    }), status_code

def update_budget():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type, entity_ids, budget = d.get("entity_type"), d.get("entity_ids", []), int(d.get("budget", 0))
    use_daily_budget = bool(d.get("use_daily_budget", budget > 0))
    results = {"success": 0, "fail": 0}
    for eid in entity_ids:
        uri = f"/ncc/campaigns/{str(eid).strip()}" if entity_type == "campaign" else f"/ncc/adgroups/{str(eid).strip()}"
        r_get = _do_req("GET", api_key, secret_key, cid, uri)
        if r_get.status_code != 200:
            results["fail"] += 1
            continue
        obj = r_get.json()
        obj["useDailyBudget"] = use_daily_budget
        obj["dailyBudget"] = budget if use_daily_budget else 0
        if "budget" in obj:
            del obj["budget"]
        r_put = _do_req("PUT", api_key, secret_key, cid, uri, params={"fields": "budget"}, json_body=obj)
        if r_put.status_code == 200:
            results["success"] += 1
        else:
            results["fail"] += 1
    return jsonify({"ok": True, "message": f"총 {len(entity_ids)}개 예산 업데이트 성공: {results['success']}개 / 실패: {results['fail']}개"})
def _normalize_schedule_days(values: Any) -> List[int]:
    out: List[int] = []
    if not isinstance(values, list):
        return out
    for v in values:
        try:
            n = int(v)
        except Exception:
            continue
        if n in DAY_NUM_TO_CODE and n not in out:
            out.append(n)
    return out
def _normalize_schedule_hours(values: Any) -> List[int]:
    """Return selected one-hour slots as 0~23 integers.

    선택한 시간 슬롯은 그대로 보존하고, 코드 생성 단계에서 연속 슬롯을
    14~16처럼 하나의 구간으로 병합합니다.
    """
    if not isinstance(values, list):
        return []
    out: List[int] = []
    seen = set()
    for v in values:
        try:
            n = int(v)
        except Exception:
            continue
        if 0 <= n <= 23 and n not in seen:
            seen.add(n)
            out.append(n)
    out.sort()
    return out

def _merge_schedule_hour_ranges(hours: List[int]) -> List[Tuple[int, int]]:
    """Merge hour slots into [start, end) ranges.

    [14, 15] -> [(14, 16)]
    [8, 9, 10, 11] -> [(8, 12)]
    [0, 1, 22, 23] -> [(0, 2), (22, 24)]
    """
    uniq: List[int] = []
    seen = set()
    for v in hours or []:
        try:
            h = int(v)
        except Exception:
            continue
        if 0 <= h <= 23 and h not in seen:
            seen.add(h)
            uniq.append(h)
    uniq.sort()
    if not uniq:
        return []
    ranges: List[Tuple[int, int]] = []
    start = prev = uniq[0]
    for h in uniq[1:]:
        if h == prev + 1:
            prev = h
            continue
        ranges.append((start, prev + 1))
        start = prev = h
    ranges.append((start, prev + 1))
    return ranges

def _build_schedule_codes(days: List[int], hours: List[int]) -> List[str]:
    codes: List[str] = []
    hour_ranges = _merge_schedule_hour_ranges(hours)
    for d_num in days:
        day_code = DAY_NUM_TO_CODE.get(int(d_num))
        if not day_code:
            continue
        for start_h, end_h in hour_ranges:
            if 0 <= start_h <= 23 and 1 <= end_h <= 24 and end_h > start_h:
                codes.append(f"SD{day_code}{start_h:02d}{end_h:02d}")
    return codes

def _schedule_code_to_slots(code: str) -> List[Tuple[str, int]]:
    s = str(code or '').strip()
    if not re.fullmatch(r"SD[A-Z]{3}\d{4}", s):
        return []
    day_code = s[2:5]
    try:
        start_h = int(s[5:7])
        end_h = int(s[7:9])
    except Exception:
        return []
    if not (0 <= start_h <= 23 and 1 <= end_h <= 24 and end_h > start_h):
        return []
    return [(day_code, h) for h in range(start_h, end_h)]

def _schedule_map_to_slot_map(schedule_map: Dict[str, int]) -> Dict[Tuple[str, int], int]:
    slots: Dict[Tuple[str, int], int] = {}
    for code, weight in (schedule_map or {}).items():
        try:
            w = int(weight or 100)
        except Exception:
            w = 100
        for slot in _schedule_code_to_slots(str(code)):
            slots[slot] = w
    return slots

def _collapse_schedule_slot_map(slot_map: Dict[Tuple[str, int], int]) -> Dict[str, int]:
    day_order = {v: i for i, v in DAY_NUM_TO_CODE.items()}
    grouped: Dict[Tuple[str, int], List[int]] = defaultdict(list)
    for (day_code, hour), weight in (slot_map or {}).items():
        try:
            h = int(hour)
            w = int(weight or 100)
        except Exception:
            continue
        if day_code not in day_order or not (0 <= h <= 23):
            continue
        grouped[(day_code, w)].append(h)
    collapsed: Dict[str, int] = {}
    for (day_code, weight), grouped_hours in sorted(
        grouped.items(),
        key=lambda item: (day_order.get(item[0][0], 99), min(item[1]) if item[1] else 99, item[0][1]),
    ):
        for start_h, end_h in _merge_schedule_hour_ranges(grouped_hours):
            collapsed[f"SD{day_code}{start_h:02d}{end_h:02d}"] = int(weight)
    return collapsed

def _normalize_schedule_blocks(values: Any) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    if not isinstance(values, list):
        return blocks
    for item in values:
        if not isinstance(item, dict):
            continue
        try:
            start_h = int(item.get("startHour"))
            end_h = int(item.get("endHour"))
            bid_weight = int(item.get("bidWeight", 100))
        except Exception:
            continue
        if not (0 <= start_h <= 23):
            continue
        if not (1 <= end_h <= 24):
            continue
        if end_h <= start_h:
            continue
        if not (10 <= bid_weight <= 500):
            continue
        block_days = _normalize_schedule_days(item.get("days") or [])
        blocks.append({"startHour": start_h, "endHour": end_h, "bidWeight": bid_weight, "days": block_days})
    return blocks

def _build_schedule_weighted_codes(days: List[int], hours: List[int], bid_weight: int = 100, schedule_blocks: Optional[List[Dict[str, Any]]] = None) -> List[Tuple[str, int]]:
    slot_map: Dict[Tuple[str, int], int] = {}
    if schedule_blocks:
        for block in schedule_blocks:
            block_days = _normalize_schedule_days(block.get("days") or []) or list(days or [])
            start_h = int(block.get("startHour", 0))
            end_h = int(block.get("endHour", 0))
            bw = int(block.get("bidWeight", 100))
            if not (0 <= start_h <= 23 and 1 <= end_h <= 24 and end_h > start_h):
                continue
            for d_num in block_days:
                day_code = DAY_NUM_TO_CODE.get(int(d_num))
                if not day_code:
                    continue
                for h in range(start_h, end_h):
                    if 0 <= h <= 23:
                        slot_map[(day_code, h)] = bw
    else:
        for d_num in days:
            day_code = DAY_NUM_TO_CODE.get(int(d_num))
            if not day_code:
                continue
            for h in _normalize_schedule_hours(hours):
                slot_map[(day_code, h)] = int(bid_weight)
    collapsed = _collapse_schedule_slot_map(slot_map)
    return [(code, weight) for code, weight in collapsed.items()]

def _extract_schedule_weight_map(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows or []:
        code = str((row or {}).get("dictionaryCode") or (row or {}).get("code") or "").strip()
        if not code:
            continue
        try:
            out[code] = int((row or {}).get("bidWeight") or 100)
        except Exception:
            out[code] = 100
    return out

def _apply_schedule_action(existing_map: Dict[str, int], incoming_weighted_codes: List[Tuple[str, int]], action_mode: str) -> Dict[str, int]:
    mode = str(action_mode or "overwrite").strip().lower()
    incoming_map: Dict[str, int] = {}
    for code, weight in incoming_weighted_codes or []:
        s = str(code or "").strip()
        if not s:
            continue
        try:
            incoming_map[s] = int(weight)
        except Exception:
            incoming_map[s] = 100

    incoming_slots = _schedule_map_to_slot_map(incoming_map)
    if mode == "overwrite":
        return _collapse_schedule_slot_map(incoming_slots)

    base_slots = _schedule_map_to_slot_map(existing_map or {})
    if mode == "add":
        base_slots.update(incoming_slots)
        return _collapse_schedule_slot_map(base_slots)
    if mode == "delete":
        for slot in incoming_slots.keys():
            base_slots.pop(slot, None)
        return _collapse_schedule_slot_map(base_slots)
    return _collapse_schedule_slot_map(incoming_slots)

def _put_schedule_weight_map(api_key: str, secret_key: str, cid: str, owner_id: str, final_map: Dict[str, int]):
    owner_id = str(owner_id or "").strip()
    final_codes = _unique_keep_order(list((final_map or {}).keys()))
    uri = f"/ncc/criterion/{owner_id}/SD"
    target_body = [{"customerId": int(cid), "ownerId": owner_id, "dictionaryCode": c, "type": "SD"} for c in final_codes]
    put_res = _do_req("PUT", api_key, secret_key, cid, uri, json_body=target_body)
    if put_res.status_code != 200:
        return False, "criterion_put", put_res.text
    weight_map: Dict[int, List[str]] = {}
    for code in final_codes:
        try:
            weight = int((final_map or {}).get(code, 100) or 100)
        except Exception:
            weight = 100
        weight_map.setdefault(weight, []).append(code)
    for weight, weight_codes in weight_map.items():
        if not weight_codes or int(weight) == 100:
            continue
        for i in range(0, len(weight_codes), 50):
            chunk = weight_codes[i:i + 50]
            bw_res = _do_req(
                "PUT",
                api_key,
                secret_key,
                cid,
                f"/ncc/criterion/{owner_id}/bidWeight",
                params={"codes": ",".join(chunk), "bidWeight": int(weight)},
            )
            if bw_res.status_code != 200:
                return False, "bid_weight_put", bw_res.text
    return True, "", ""

def update_schedule():
    d = request.get_json(silent=True) or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    action_mode = str(d.get("actionMode") or "overwrite").strip().lower()
    if action_mode not in {"overwrite", "add", "delete"}:
        action_mode = "overwrite"
    adgroup_ids = _unique_keep_order([str(x).strip() for x in (d.get("adgroup_ids") or []) if str(x).strip()])
    days = _normalize_schedule_days(d.get("days") or [])
    raw_hours = []
    for x in (d.get("hours") or []):
        try:
            n = int(x)
        except Exception:
            continue
        if 0 <= n <= 23:
            raw_hours.append(n)
    raw_hours = sorted(set(raw_hours))
    hours = _normalize_schedule_hours(d.get("hours") or [])
    schedule_blocks = _normalize_schedule_blocks(d.get("scheduleBlocks") or [])
    try:
        bid_weight = int(d.get("bidWeight", 100))
    except Exception:
        bid_weight = 100
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not adgroup_ids:
        return jsonify({"error": "광고그룹이 선택되지 않았습니다."}), 400
    if not days and not any(block.get("days") for block in schedule_blocks):
        return jsonify({"error": "요일을 1개 이상 선택해주세요."}), 400
    if not schedule_blocks and not raw_hours:
        return jsonify({"error": "시간을 1개 이상 선택해주세요."}), 400
    if not schedule_blocks and not hours:
        return jsonify({"error": "적용할 시간대가 없습니다. 예: 14·15 선택 → 실제 적용 14~16"}), 400
    weighted_codes = _build_schedule_weighted_codes(days, hours, bid_weight, schedule_blocks)
    codes = [code for code, _ in weighted_codes]
    if not codes and action_mode != "delete":
        return jsonify({"error": "적용할 시간대 코드가 없습니다."}), 400
    results = []
    success = 0
    fail = 0
    fetch_errors = []
    for owner_id in adgroup_ids:
        owner_id = str(owner_id).strip()
        existing_map: Dict[str, int] = {}
        if action_mode in {"add", "delete"}:
            res_existing, existing_rows = _fetch_schedule_entries(api_key, secret_key, cid, owner_id)
            if res_existing.status_code not in {200, 404}:
                fail += 1
                fetch_errors.append(f"[{owner_id}] 기존 스케줄 조회 실패: {res_existing.text}")
                results.append({"ownerId": owner_id, "ok": False, "step": "criterion_fetch", "details": res_existing.text})
                continue
            existing_map = _extract_schedule_weight_map(existing_rows or [])
        final_map = _apply_schedule_action(existing_map, weighted_codes, action_mode)
        ok_put, step, detail = _put_schedule_weight_map(api_key, secret_key, cid, owner_id, final_map)
        if not ok_put:
            fail += 1
            results.append({
                "ownerId": owner_id,
                "ok": False,
                "step": step,
                "details": detail,
                "codes": list(final_map.keys()),
            })
            continue
        success += 1
        results.append({"ownerId": owner_id, "ok": True, "codes": list(final_map.keys()), "weightMap": final_map, "actionMode": action_mode})
    status_code = 200 if success > 0 else 400
    action_label = {"overwrite": "덮어쓰기", "add": "추가", "delete": "삭제"}.get(action_mode, "덮어쓰기")
    return jsonify({
        "ok": success > 0,
        "message": f"총 {len(adgroup_ids)}개 스케줄 {action_label} 성공: {success}개 / 실패: {fail}개",
        "success": success,
        "fail": fail,
        "action_mode": action_mode,
        "raw_hours": raw_hours,
        "applied_hours": hours,
        "schedule_blocks": schedule_blocks,
        "fetch_errors": fetch_errors[:10],
        "results": results[:20],
    }), status_code

def update_schedule_campaign_bulk():
    d = request.get_json(silent=True) or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    action_mode = str(d.get("actionMode") or "overwrite").strip().lower()
    if action_mode not in {"overwrite", "add", "delete"}:
        action_mode = "overwrite"
    campaign_ids = _unique_keep_order([str(x).strip() for x in (d.get("campaign_ids") or []) if str(x).strip()])
    days = _normalize_schedule_days(d.get("days") or [])
    raw_hours = []
    for x in (d.get("hours") or []):
        try:
            n = int(x)
        except Exception:
            continue
        if 0 <= n <= 23:
            raw_hours.append(n)
    raw_hours = sorted(set(raw_hours))
    hours = _normalize_schedule_hours(d.get("hours") or [])
    schedule_blocks = _normalize_schedule_blocks(d.get("scheduleBlocks") or [])
    try:
        bid_weight = int(d.get("bidWeight", 100))
    except Exception:
        bid_weight = 100
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not campaign_ids:
        return jsonify({"error": "캠페인이 선택되지 않았습니다."}), 400
    if not days and not any(block.get("days") for block in schedule_blocks):
        return jsonify({"error": "요일을 1개 이상 선택해주세요."}), 400
    if not schedule_blocks and not raw_hours:
        return jsonify({"error": "시간을 1개 이상 선택해주세요."}), 400
    if not schedule_blocks and not hours:
        return jsonify({"error": "적용할 시간대가 없습니다. 예: 14·15 선택 → 실제 적용 14~16"}), 400
    adgroup_ids = []
    fetch_errors = []
    for camp_id in campaign_ids:
        r_adgs = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": camp_id})
        if r_adgs.status_code == 200:
            adgroup_ids.extend([adg.get("nccAdgroupId") for adg in r_adgs.json() or [] if adg.get("nccAdgroupId")])
        else:
            fetch_errors.append(f"[{camp_id}] {r_adgs.text}")
    adgroup_ids = _unique_keep_order(adgroup_ids)
    if not adgroup_ids:
        return jsonify({"error": "하위 광고그룹을 불러오지 못했습니다.", "details": fetch_errors[:10]}), 400
    weighted_codes = _build_schedule_weighted_codes(days, hours, bid_weight, schedule_blocks)
    codes = [code for code, _ in weighted_codes]
    if not codes and action_mode != "delete":
        return jsonify({"error": "적용할 시간대 코드가 없습니다."}), 400
    results = []
    success = 0
    fail = 0
    for owner_id in adgroup_ids:
        owner_id = str(owner_id).strip()
        existing_map: Dict[str, int] = {}
        if action_mode in {"add", "delete"}:
            res_existing, existing_rows = _fetch_schedule_entries(api_key, secret_key, cid, owner_id)
            if res_existing.status_code not in {200, 404}:
                fail += 1
                fetch_errors.append(f"[{owner_id}] 기존 스케줄 조회 실패: {res_existing.text}")
                results.append({"ownerId": owner_id, "ok": False, "step": "criterion_fetch", "details": res_existing.text})
                continue
            existing_map = _extract_schedule_weight_map(existing_rows or [])
        final_map = _apply_schedule_action(existing_map, weighted_codes, action_mode)
        ok_put, step, detail = _put_schedule_weight_map(api_key, secret_key, cid, owner_id, final_map)
        if not ok_put:
            fail += 1
            results.append({"ownerId": owner_id, "ok": False, "step": step, "details": detail, "codes": list(final_map.keys())})
            continue
        success += 1
        results.append({"ownerId": owner_id, "ok": True, "codes": list(final_map.keys()), "weightMap": final_map, "actionMode": action_mode})
    status_code = 200 if success > 0 else 400
    action_label = {"overwrite": "덮어쓰기", "add": "추가", "delete": "삭제"}.get(action_mode, "덮어쓰기")
    return jsonify({
        "ok": success > 0,
        "message": f"하위 광고그룹 총 {len(adgroup_ids)}개 스케줄 {action_label} 성공: {success}개 / 실패: {fail}개",
        "success": success,
        "fail": fail,
        "action_mode": action_mode,
        "raw_hours": raw_hours,
        "applied_hours": hours,
        "schedule_blocks": schedule_blocks,
        "fetch_errors": fetch_errors[:10],
        "results": results[:20],
    }), status_code

def _unique_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        s = str(v or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out
def _resolve_adgroup_ids(api_key: str, secret_key: str, cid: str, entity_type: str, entity_ids: List[str]):
    adgroup_ids: List[str] = []
    warnings: List[str] = []
    entity_type = str(entity_type or "adgroup").strip().lower()
    entity_ids = _unique_keep_order(entity_ids or [])
    if entity_type == "campaign":
        for camp_id in entity_ids:
            res, rows = _fetch_adgroups(api_key, secret_key, cid, str(camp_id).strip())
            if res.status_code != 200:
                warnings.append(f"캠페인 {camp_id} 하위 광고그룹 조회 실패: {res.text}")
                continue
            for row in rows:
                adg_id = str(row.get("id") or row.get("nccAdgroupId") or "").strip()
                if adg_id:
                    adgroup_ids.append(adg_id)
    else:
        adgroup_ids.extend([str(x).strip() for x in (entity_ids or []) if str(x).strip()])
    return _unique_keep_order(adgroup_ids), warnings
def _resolve_copy_source_adgroup_ids(api_key: str, secret_key: str, cid: str, source_ids: List[str], source_campaign_ids: List[str]):
    direct_ids = _unique_keep_order(source_ids or [])
    campaign_adgroup_ids, warnings = _resolve_adgroup_ids(api_key, secret_key, cid, "campaign", source_campaign_ids or [])
    resolved = _unique_keep_order(direct_ids + campaign_adgroup_ids)
    return resolved, warnings
def _resolve_web_site_adgroup_ids(api_key: str, secret_key: str, cid: str, entity_type: str, entity_ids: List[str]):
    adgroup_ids: List[str] = []
    skipped_non_web = 0
    warnings: List[str] = []
    if entity_type == "campaign":
        for camp_id in entity_ids:
            res, rows = _fetch_adgroups(api_key, secret_key, cid, str(camp_id).strip())
            if res.status_code != 200:
                warnings.append(f"캠페인 {camp_id} 하위 광고그룹 조회 실패: {res.text}")
                continue
            for row in rows:
                if str(row.get("adgroupType") or "").upper() == "WEB_SITE":
                    adgroup_ids.append(str(row.get("id") or "").strip())
                else:
                    skipped_non_web += 1
    else:
        for adg_id in entity_ids:
            res, obj = _fetch_adgroup_detail(api_key, secret_key, cid, str(adg_id).strip())
            if res.status_code != 200 or not obj:
                warnings.append(f"광고그룹 {adg_id} 조회 실패: {res.text}")
                continue
            if str(obj.get("adgroupType") or "").upper() == "WEB_SITE":
                adgroup_ids.append(str(obj.get("nccAdgroupId") or adg_id).strip())
            else:
                skipped_non_web += 1
    return _unique_keep_order(adgroup_ids), skipped_non_web, warnings
def _resolve_shopping_adgroup_ids(api_key: str, secret_key: str, cid: str, entity_type: str, entity_ids: List[str]):
    adgroup_ids: List[str] = []
    skipped_non_shopping = 0
    warnings: List[str] = []
    entity_type = str(entity_type or "adgroup").strip().lower()
    entity_ids = _unique_keep_order(entity_ids or [])
    if entity_type == "campaign":
        for camp_id in entity_ids:
            res, rows = _fetch_adgroups(api_key, secret_key, cid, str(camp_id).strip())
            if res.status_code != 200:
                warnings.append(f"캠페인 {camp_id} 하위 광고그룹 조회 실패: {res.text}")
                continue
            for row in rows:
                adgroup_type = str(row.get("adgroupType") or "").upper()
                if adgroup_type in SHOPPING_TARGETABLE_ADGROUP_TYPES:
                    adgroup_ids.append(str(row.get("id") or row.get("nccAdgroupId") or "").strip())
                else:
                    skipped_non_shopping += 1
    else:
        for adg_id in entity_ids:
            res, obj = _fetch_adgroup_detail(api_key, secret_key, cid, str(adg_id).strip())
            if res.status_code != 200 or not obj:
                warnings.append(f"광고그룹 {adg_id} 조회 실패: {res.text}")
                continue
            adgroup_type = str(obj.get("adgroupType") or "").upper()
            if adgroup_type in SHOPPING_TARGETABLE_ADGROUP_TYPES:
                adgroup_ids.append(str(obj.get("nccAdgroupId") or adg_id).strip())
            else:
                skipped_non_shopping += 1
    return _unique_keep_order(adgroup_ids), skipped_non_shopping, warnings
def _normalize_bid_amt(value: Any, max_bid: Optional[int] = None) -> Optional[int]:
    try:
        bid = int(float(value))
    except Exception:
        return None
    bid = int(round(bid / 10.0) * 10)
    if max_bid is not None:
        bid = min(bid, int(max_bid))
    bid = max(70, min(100000, bid))
    return bid

def _normalize_estimated_bid_amt(value: Any, max_bid: Optional[int] = None) -> Optional[int]:
    try:
        bid = float(value)
    except Exception:
        return None
    if bid <= 0:
        return None
    bid = int(round(bid / 10.0) * 10)
    if max_bid is not None:
        bid = min(bid, int(max_bid))
    bid = max(70, min(100000, bid))
    return bid
def _normalize_bid_weight(value: Any) -> Optional[int]:
    try:
        weight = int(float(value))
    except Exception:
        return None
    return max(10, min(500, weight))
def _normalize_optional_bid_weight(value: Any) -> Optional[int]:
    if value is None:
        return None
    if str(value).strip() == "":
        return None
    try:
        weight = int(float(value))
    except Exception:
        return None
    if not (10 <= weight <= 500):
        return None
    return weight

def _safe_int_default(value: Any, default: int = 100) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)

def _is_not_support_modify_field_response(res: Any) -> bool:
    try:
        text = str(getattr(res, "text", "") or "")
    except Exception:
        text = ""
    return ("Not support modify field" in text) or ('"code":3726' in text.replace(" ", "")) or ("code=3726" in text)


def _device_bid_weight_strategy_candidates(pc_weight: Optional[int], mobile_weight: Optional[int], preferred_strategy: Optional[str] = None) -> List[Dict[str, Any]]:
    # PC/MO 입찰가중치는 `fields=pcNetworkBidWeight` 형태의 부분수정에서 3726이 날 수 있다.
    # 그래서 실제 광고그룹 객체를 조회한 뒤 값만 바꿔 전체 update(no fields)를 먼저 시도한다.
    strategies: List[Dict[str, Any]] = []
    target_parts: List[str] = []
    if pc_weight is not None:
        target_parts.append("pcNetworkBidWeight")
    if mobile_weight is not None:
        target_parts.append("mobileNetworkBidWeight")
    if not target_parts:
        return strategies

    strategies.append({"kind": "full", "fields": "__FULL_SANITIZED__"})
    strategies.append({"kind": "full", "fields": "__FULL_RAW__"})
    if pc_weight is not None and mobile_weight is not None:
        strategies.append({"kind": "combined", "fields": "pcNetworkBidWeight,mobileNetworkBidWeight"})
        strategies.append({"kind": "combined", "fields": "networkBidWeight"})
        strategies.append({"kind": "separate", "fields": "pcNetworkBidWeight|mobileNetworkBidWeight"})
    elif pc_weight is not None:
        strategies.append({"kind": "combined", "fields": "pcNetworkBidWeight"})
    elif mobile_weight is not None:
        strategies.append({"kind": "combined", "fields": "mobileNetworkBidWeight"})

    if preferred_strategy:
        preferred_strategy = str(preferred_strategy or "").strip()
        preferred = [x for x in strategies if str(x.get("fields") or "") == preferred_strategy]
        rest = [x for x in strategies if str(x.get("fields") or "") != preferred_strategy]
        if preferred:
            return preferred + rest
    return strategies


def _verify_powerlink_device_bid_weight(api_key: str, secret_key: str, cid: str, adgroup_id: str, target_pc: int, target_mobile: int):
    _, verify_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
    if not isinstance(verify_obj, dict):
        return None, None, None
    verified_pc = _safe_int_default(verify_obj.get("pcNetworkBidWeight"), target_pc)
    verified_mobile = _safe_int_default(verify_obj.get("mobileNetworkBidWeight"), target_mobile)
    return verified_pc, verified_mobile, verify_obj


def _prepare_powerlink_device_bid_weight_update_obj(obj: Dict[str, Any], cid: str, target_pc: int, target_mobile: int, sanitized: bool = True) -> Dict[str, Any]:
    update_obj = dict(obj or {})
    update_obj["pcNetworkBidWeight"] = int(target_pc)
    update_obj["mobileNetworkBidWeight"] = int(target_mobile)
    update_obj.setdefault("customerId", _safe_int_default(cid, 0))
    adgroup_id = str(update_obj.get("nccAdgroupId") or update_obj.get("id") or "").strip()
    if adgroup_id:
        update_obj["nccAdgroupId"] = adgroup_id

    # PC/MO 가중치와 무관한 콘텐츠 네트워크 값은 기존 값 유지. 누락 시에만 안전 기본값을 채운다.
    update_obj.setdefault("useCntsNetworkBidWeight", bool(obj.get("useCntsNetworkBidWeight", False)))
    update_obj.setdefault("contentsNetworkBidWeight", _safe_int_default(obj.get("contentsNetworkBidWeight"), 100))
    update_obj.setdefault("useCntsNetworkBidAmt", bool(obj.get("useCntsNetworkBidAmt", False)))
    update_obj.setdefault("contentsNetworkBidAmt", _safe_int_default(obj.get("contentsNetworkBidAmt"), _safe_int_default(obj.get("bidAmt"), 70)))

    if not sanitized:
        return update_obj

    # 전체 update(no fields)는 읽기전용/요약성 필드를 같이 보내면 계정/상품에 따라 실패할 수 있어 제거한다.
    readonly_keys = {
        "id", "raw", "targets", "targetSummary", "pcChannelKey", "mobileChannelKey",
        "status", "statusReason", "expectCost", "regTm", "editTm", "migType",
        "sharedDailyBudget", "sharedBudgetName", "sharedBudgetLock", "sharedBudgetExpectCost",
        "numberInUse", "pcDevice", "mobileDevice",
    }
    for key in readonly_keys:
        update_obj.pop(key, None)
    return {k: v for k, v in update_obj.items() if v is not None}


def _put_powerlink_device_bid_weight_full(api_key: str, secret_key: str, cid: str, adgroup_id: str, obj: Dict[str, Any], target_pc: int, target_mobile: int, sanitized: bool = True):
    update_obj = _prepare_powerlink_device_bid_weight_update_obj(obj, cid, target_pc, target_mobile, sanitized=sanitized)
    return _do_req(
        "PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}",
        params=None, json_body=update_obj
    )


def _put_powerlink_device_bid_weight_combined(api_key: str, secret_key: str, cid: str, adgroup_id: str, obj: Dict[str, Any], target_pc: int, target_mobile: int, fields: str):
    update_obj = _prepare_powerlink_device_bid_weight_update_obj(obj, cid, target_pc, target_mobile, sanitized=False)
    return _do_req(
        "PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}",
        params={"fields": fields}, json_body=update_obj
    )


def _put_powerlink_device_bid_weight_separate(api_key: str, secret_key: str, cid: str, adgroup_id: str, obj: Dict[str, Any], current_pc: int, current_mobile: int, target_pc: int, target_mobile: int):
    working_obj = _prepare_powerlink_device_bid_weight_update_obj(obj, cid, current_pc, current_mobile, sanitized=False)
    changed_parts: List[str] = []
    last_res = None
    if current_pc != target_pc:
        working_obj["pcNetworkBidWeight"] = int(target_pc)
        working_obj.setdefault("mobileNetworkBidWeight", int(current_mobile))
        res_pc = _do_req(
            "PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}",
            params={"fields": "pcNetworkBidWeight"}, json_body=working_obj
        )
        last_res = res_pc
        if res_pc.status_code not in [200, 201]:
            return False, res_pc, changed_parts
        changed_parts.append("pcNetworkBidWeight")
        try:
            maybe_obj = res_pc.json()
            if isinstance(maybe_obj, dict):
                working_obj = maybe_obj
        except Exception:
            pass
    if current_mobile != target_mobile:
        working_obj["mobileNetworkBidWeight"] = int(target_mobile)
        working_obj.setdefault("pcNetworkBidWeight", int(target_pc))
        res_mo = _do_req(
            "PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}",
            params={"fields": "mobileNetworkBidWeight"}, json_body=working_obj
        )
        last_res = res_mo
        if res_mo.status_code not in [200, 201]:
            return False, res_mo, changed_parts
        changed_parts.append("mobileNetworkBidWeight")
    return True, last_res, changed_parts


def _update_powerlink_device_bid_weight_for_adgroup(api_key: str, secret_key: str, cid: str, adgroup_id: str, pc_weight: Optional[int], mobile_weight: Optional[int], preferred_strategy: Optional[str] = None):
    res_get, obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
    if res_get.status_code != 200 or not isinstance(obj, dict):
        detail = res_get.text if res_get is not None else "광고그룹 조회 실패"
        return False, f"광고그룹 조회 실패: {detail}", None, {}
    if str(obj.get("adgroupType") or "").upper() != "WEB_SITE":
        return None, "파워링크 광고그룹이 아니어서 건너뜀", obj, {}

    current_pc = _safe_int_default(obj.get("pcNetworkBidWeight"), 100)
    current_mobile = _safe_int_default(obj.get("mobileNetworkBidWeight"), 100)
    target_pc = int(pc_weight) if pc_weight is not None else current_pc
    target_mobile = int(mobile_weight) if mobile_weight is not None else current_mobile

    if current_pc == target_pc and current_mobile == target_mobile:
        return True, f"이미 동일값 유지(PC {current_pc}%, MO {current_mobile}%)", obj, {"strategy": "unchanged"}

    strategies = _device_bid_weight_strategy_candidates(pc_weight, mobile_weight, preferred_strategy)
    if not strategies:
        return True, "변경사항 없음", obj, {"strategy": "none"}

    unsupported_details: List[str] = []
    failed_details: List[str] = []
    last_error = ""
    for strategy in strategies:
        kind = str(strategy.get("kind") or "combined")
        fields = str(strategy.get("fields") or "")
        if kind == "full":
            sanitized = fields != "__FULL_RAW__"
            res_put = _put_powerlink_device_bid_weight_full(
                api_key, secret_key, cid, adgroup_id, obj, target_pc, target_mobile, sanitized=sanitized
            )
            if res_put.status_code in [200, 201]:
                verified_pc, verified_mobile, verify_obj = _verify_powerlink_device_bid_weight(api_key, secret_key, cid, adgroup_id, target_pc, target_mobile)
                if verified_pc is not None and (verified_pc != target_pc or verified_mobile != target_mobile):
                    return False, f"API 응답은 성공이나 반영값 불일치(요청 PC {target_pc}%/MO {target_mobile}%, 확인 PC {verified_pc}%/MO {verified_mobile}%)", verify_obj or obj, {"strategy": fields}
                return True, f"PC {current_pc}%→{target_pc}% / MO {current_mobile}%→{target_mobile}% (전체 update)", verify_obj or obj, {"strategy": fields}
            last_error = res_put.text if res_put is not None else "입찰가중치 변경 실패"
            failed_details.append(f"{fields}: {last_error}")
            if _is_not_support_modify_field_response(res_put):
                unsupported_details.append(f"{fields}: {last_error}")
            continue

        if kind == "separate":
            ok, res_put, changed_parts = _put_powerlink_device_bid_weight_separate(
                api_key, secret_key, cid, adgroup_id, obj, current_pc, current_mobile, target_pc, target_mobile
            )
            if ok:
                verified_pc, verified_mobile, verify_obj = _verify_powerlink_device_bid_weight(api_key, secret_key, cid, adgroup_id, target_pc, target_mobile)
                if verified_pc is not None and (verified_pc != target_pc or verified_mobile != target_mobile):
                    return False, f"API 응답은 성공이나 반영값 불일치(요청 PC {target_pc}%/MO {target_mobile}%, 확인 PC {verified_pc}%/MO {verified_mobile}%)", verify_obj or obj, {"strategy": fields}
                return True, f"PC {current_pc}%→{target_pc}% / MO {current_mobile}%→{target_mobile}%", verify_obj or obj, {"strategy": fields}
            detail = getattr(res_put, "text", "") if res_put is not None else "입찰가중치 변경 실패"
            last_error = str(detail or "")
            failed_details.append(f"fields={fields}: {last_error}")
            if _is_not_support_modify_field_response(res_put):
                unsupported_details.append(f"fields={fields}: {last_error}")
                continue
            if changed_parts:
                return False, f"일부 필드({', '.join(changed_parts)}) 적용 후 실패: {last_error}", obj, {"strategy": fields}
            continue

        res_put = _put_powerlink_device_bid_weight_combined(
            api_key, secret_key, cid, adgroup_id, obj, target_pc, target_mobile, fields
        )
        if res_put.status_code in [200, 201]:
            verified_pc, verified_mobile, verify_obj = _verify_powerlink_device_bid_weight(api_key, secret_key, cid, adgroup_id, target_pc, target_mobile)
            if verified_pc is not None and (verified_pc != target_pc or verified_mobile != target_mobile):
                return False, f"API 응답은 성공이나 반영값 불일치(요청 PC {target_pc}%/MO {target_mobile}%, 확인 PC {verified_pc}%/MO {verified_mobile}%)", verify_obj or obj, {"strategy": fields}
            return True, f"PC {current_pc}%→{target_pc}% / MO {current_mobile}%→{target_mobile}%", verify_obj or obj, {"strategy": fields}
        last_error = res_put.text if res_put is not None else "입찰가중치 변경 실패"
        failed_details.append(f"fields={fields}: {last_error}")
        if _is_not_support_modify_field_response(res_put):
            unsupported_details.append(f"fields={fields}: {last_error}")
            continue
        continue

    compact_error = " / ".join(failed_details[:3]) if failed_details else last_error
    unsupported_only = bool(unsupported_details) and len(unsupported_details) == len(failed_details)
    return False, (
        "PC/모바일 입찰가중치 변경 실패. "
        + ("지원 필드 방식이 모두 거절되었습니다. " if unsupported_only else "")
        + f"마지막 응답: {compact_error}"
    ), obj, {"unsupported_field": unsupported_only, "strategy": "unsupported"}

def _calc_bid_stats(values: List[int]):
    if not values:
        return {"min": None, "max": None, "median": None}
    vals = sorted(values)
    n = len(vals)
    if n % 2:
        median = vals[n // 2]
    else:
        median = int(round((vals[n // 2 - 1] + vals[n // 2]) / 2.0 / 10.0) * 10)
    return {"min": vals[0], "max": vals[-1], "median": median}
def _is_keyword_editable_for_avg_position(row: Dict[str, Any], include_paused: bool = False, include_pending: bool = False):
    if bool(row.get("delFlag")):
        return False, "삭제됨"
    status = str(row.get("status") or "").upper()
    if (not include_paused) and status in {"PAUSE", "PAUSED", "STOP", "STOPPED", "LIMITEDBYBUDGET"}:
        return False, f"상태:{status or 'PAUSE'}"
    inspect = str(row.get("inspectStatus") or "").upper()
    if (not include_pending) and inspect and inspect not in {"APPROVED", "NONE", "NORMAL"}:
        return False, f"검수:{inspect}"
    return True, ""
def _is_ad_editable_for_avg_position(row: Dict[str, Any], include_paused: bool = False, include_pending: bool = False):
    if bool(row.get("delFlag")):
        return False, "삭제됨"
    status = str(row.get("status") or "").upper()
    if (not include_paused) and status in {"PAUSE", "PAUSED", "STOP", "STOPPED", "LIMITEDBYBUDGET"}:
        return False, f"상태:{status or 'PAUSE'}"
    inspect = str(row.get("inspectStatus") or "").upper()
    if (not include_pending) and inspect and inspect not in {"APPROVED", "NONE", "NORMAL"}:
        return False, f"검수:{inspect}"
    return True, ""
def _estimate_keyword_bids_by_avg_position(api_key: str, secret_key: str, cid: str, keyword_ids: List[str], device: str, position: int, max_bid: Optional[int] = None, keyword_meta: Optional[Dict[str, Dict[str, Any]]] = None):
    estimated: Dict[str, int] = {}
    warnings: List[str] = []
    device = str(device or "PC").upper()
    keyword_meta = keyword_meta or {}

    text_to_ids: Dict[str, List[str]] = {}
    ordered_texts: List[str] = []
    for kid in keyword_ids:
        kid = str(kid or "").strip()
        if not kid:
            continue
        kw_text = str((keyword_meta.get(kid) or {}).get("keyword") or "").strip()
        if not kw_text:
            continue
        if kw_text not in text_to_ids:
            text_to_ids[kw_text] = []
            ordered_texts.append(kw_text)
        text_to_ids[kw_text].append(kid)

    text_estimated = False
    for i in range(0, len(ordered_texts), 100):
        chunk = [str(x).strip() for x in ordered_texts[i:i + 100] if str(x).strip()]
        if not chunk:
            continue
        body = {
            "device": device,
            "items": [{"key": kw, "position": int(position)} for kw in chunk],
        }
        res = _do_req("POST", api_key, secret_key, cid, "/estimate/average-position-bid/keyword", json_body=body)
        if res.status_code != 200:
            if len(warnings) < 10:
                warnings.append(f"파워링크 텍스트 기준 평균순위 추정 실패({device}, {position}위): {res.text}")
            text_estimated = False
            break
        payload = res.json() or {}
        items = None
        if isinstance(payload, dict):
            items = payload.get("items")
            if not isinstance(items, list):
                items = payload.get("estimate")
        elif isinstance(payload, list):
            items = payload
        if not isinstance(items, list):
            if len(warnings) < 10:
                warnings.append(f"파워링크 텍스트 기준 평균순위 추정 응답 형식이 예상과 다릅니다: {payload}")
            text_estimated = False
            break
        text_estimated = True
        for item in items:
            try:
                kw_text = str(item.get("keyword") or item.get("key") or "").strip()
                bid = _normalize_estimated_bid_amt(item.get("bid"), max_bid=max_bid)
            except Exception:
                kw_text = ""
                bid = None
            if not kw_text or bid is None:
                continue
            for kid in text_to_ids.get(kw_text, []):
                estimated[kid] = bid

    if estimated:
        return estimated, warnings

    for i in range(0, len(keyword_ids), 100):
        chunk = [str(x).strip() for x in keyword_ids[i:i + 100] if str(x).strip()]
        if not chunk:
            continue
        body = {
            "device": device,
            "items": [{"key": kid, "position": int(position)} for kid in chunk],
        }
        res = _do_req("POST", api_key, secret_key, cid, "/npc-estimate/average-position-bid/id", json_body=body)
        if res.status_code != 200:
            if len(warnings) < 10:
                warnings.append(f"파워링크 ID 기준 평균순위 추정 실패({device}, {position}위): {res.text}")
            continue
        payload = res.json() or {}
        items = None
        if isinstance(payload, dict):
            items = payload.get("items")
            if not isinstance(items, list):
                items = payload.get("estimate")
        elif isinstance(payload, list):
            items = payload
        if not isinstance(items, list):
            if len(warnings) < 10:
                warnings.append(f"파워링크 ID 기준 평균순위 추정 응답 형식이 예상과 다릅니다: {payload}")
            continue
        for item in items:
            try:
                key = str(item.get("key") or item.get("nccKeywordId") or "").strip()
                bid = _normalize_estimated_bid_amt(item.get("bid"), max_bid=max_bid)
            except Exception:
                bid = None
                key = ""
            if key and bid is not None:
                estimated[key] = bid
    return estimated, warnings
def _estimate_ad_bids_by_avg_position(api_key: str, secret_key: str, cid: str, ad_ids: List[str], device: str, position: int, max_bid: Optional[int] = None):
    estimated: Dict[str, int] = {}
    warnings: List[str] = []
    device = str(device or "PC").upper()
    for i in range(0, len(ad_ids), 100):
        chunk = [str(x).strip() for x in ad_ids[i:i + 100] if str(x).strip()]
        if not chunk:
            continue
        body = {
            "device": device,
            "items": [{"key": ad_id, "position": int(position)} for ad_id in chunk],
        }
        res = _do_req("POST", api_key, secret_key, cid, "/npla-estimate/average-position-bid/id", json_body=body)
        if res.status_code != 200:
            warnings.append(f"쇼핑 평균순위 추정 실패({device}, {position}위): {res.text}")
            continue
        payload = res.json() or {}
        items = None
        if isinstance(payload, dict):
            items = payload.get("items")
            if not isinstance(items, list):
                items = payload.get("estimate")
        elif isinstance(payload, list):
            items = payload
        if not isinstance(items, list):
            warnings.append(f"쇼핑 평균순위 추정 응답 형식이 예상과 다릅니다: {payload}")
            continue
        for item in items:
            try:
                key = str(item.get("key") or item.get("nccAdId") or item.get("id") or "").strip()
                bid = _normalize_bid_amt(item.get("bid") or item.get("bidAmt"), max_bid=max_bid)
            except Exception:
                bid = None
                key = ""
            if key and bid is not None:
                estimated[key] = bid
    return estimated, warnings
def _apply_keyword_bid_map(api_key: str, secret_key: str, cid: str, adgroup_ids: List[str], bid_map: Dict[str, int], keyword_meta: Optional[Dict[str, Dict[str, Any]]] = None):
    success_cnt = fail_cnt = skipped_cnt = 0
    err_details: List[str] = []
    cleanup_keys = ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']
    keyword_meta = keyword_meta or {}
    for adg_id in adgroup_ids:
        r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id})
        if r_kw.status_code != 200:
            fail_cnt += 1
            if len(err_details) < 5:
                err_details.append(f"[광고그룹 {adg_id}] 키워드 조회 실패: {r_kw.text}")
            continue
        kws = r_kw.json() or []
        update_payload = []
        for kw in kws:
            kid = str(kw.get("nccKeywordId") or "").strip()
            if not kid:
                continue
            if kid not in bid_map:
                skipped_cnt += 1
                continue
            target_bid = int(bid_map[kid])
            meta = keyword_meta.get(kid, {})
            current_bid = _normalize_bid_amt(meta.get("current_bid", kw.get("bidAmt")))
            current_use_group = bool(meta.get("use_group_bid", kw.get("useGroupBidAmt")))
            if (current_bid == target_bid) and (not current_use_group):
                skipped_cnt += 1
                continue
            item = copy.deepcopy(kw)
            item["useGroupBidAmt"] = False
            item["bidAmt"] = target_bid
            for k in cleanup_keys:
                item.pop(k, None)
            update_payload.append(item)
        for i in range(0, len(update_payload), 100):
            batch = update_payload[i:i + 100]
            if not batch:
                continue
            r_put = _do_req("PUT", api_key, secret_key, cid, "/ncc/keywords", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=batch)
            if r_put.status_code in [200, 201]:
                success_cnt += len(batch)
            else:
                for item in batch:
                    r_single = _do_req("PUT", api_key, secret_key, cid, f"/ncc/keywords/{item['nccKeywordId']}", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=item)
                    if r_single.status_code in [200, 201]:
                        success_cnt += 1
                    else:
                        fail_cnt += 1
                        if len(err_details) < 5:
                            err_details.append(f"[{item.get('keyword', '알수없음')}] 변경 실패: {r_single.text}")
    return success_cnt, fail_cnt, skipped_cnt, err_details
def update_non_search_keyword_exclusion():
    d = request.get_json(force=True) or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    entity_type = str(d.get("entity_type") or "adgroup").strip().lower()
    entity_ids = _unique_keep_order(d.get("entity_ids") or [])
    if entity_type not in {"campaign", "adgroup"}:
        return jsonify({"error": "entity_type은 campaign 또는 adgroup 이어야 합니다."}), 400
    if not entity_ids:
        return jsonify({"error": "대상이 없습니다."}), 400
    if "excluded" not in d:
        return jsonify({"error": "사용 여부(excluded)는 필수입니다."}), 400
    excluded = _boolish(d.get("excluded"), False)
    adgroup_ids, skipped_non_shopping, warnings = _resolve_shopping_adgroup_ids(api_key, secret_key, cid, entity_type, entity_ids)
    if not adgroup_ids:
        msg = "선택 범위에서 적용 가능한 쇼핑 광고그룹을 찾지 못했습니다."
        if warnings:
            msg += f" / {warnings[0]}"
        return jsonify({"error": msg}), 400
    success = 0
    fail = 0
    details: List[Dict[str, Any]] = []
    for adg_id in adgroup_ids:
        ok, msg = _update_non_search_keyword_target(api_key, secret_key, cid, adg_id, excluded)
        details.append({"adgroup_id": adg_id, "success": bool(ok), "message": msg})
        if ok:
            success += 1
        else:
            fail += 1
    message_bits = [f"검색어 없는 경우 광고 노출 제외 {'사용' if excluded else '사용 안함'} 적용 완료: 성공 {success}건 / 실패 {fail}건"]
    if skipped_non_shopping > 0:
        message_bits.append(f"비쇼핑 광고그룹 {skipped_non_shopping}건은 건너뜀")
    if warnings:
        message_bits.append(f"조회 경고 {len(warnings)}건")
    return jsonify({
        "ok": fail == 0,
        "success": success,
        "fail": fail,
        "skipped_non_shopping": skipped_non_shopping,
        "warnings": warnings,
        "results": details,
        "message": " / ".join(message_bits),
    }), (200 if fail == 0 else 207)
def preview_keyword_bids_by_search():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    search_text = str(d.get("search_text") or d.get("keyword_query") or "").strip()
    exact_match = _boolish(d.get("exact_match"), False)
    exclude_text = str(d.get("exclude_text") or d.get("exclude_keyword_query") or "").strip()
    search_scope = str(d.get("search_scope") or "account").strip().lower()
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not search_text:
        return jsonify({"error": "검색할 단어를 입력해주세요."}), 400
    if search_scope not in {"account", "selected_campaigns", "selected_adgroups"}:
        search_scope = "account"
    if search_scope == "selected_campaigns" and not campaign_ids:
        return jsonify({"error": "선택 캠페인 조회를 사용하려면 좌측에서 캠페인을 체크해주세요."}), 400
    if search_scope == "selected_adgroups" and not adgroup_ids:
        return jsonify({"error": "선택 그룹내 조회를 사용하려면 좌측에서 광고그룹을 체크해주세요."}), 400
    scan = _scan_powerlink_keywords_by_search(
        api_key,
        secret_key,
        cid,
        search_text,
        exact_match=exact_match,
        campaign_ids=(campaign_ids if search_scope == "selected_campaigns" else None),
        adgroup_ids=(adgroup_ids if search_scope == "selected_adgroups" else None),
        exclude_text=exclude_text,
    )
    powerlink_campaigns = scan["powerlink_campaigns"]
    adgroup_contexts = scan["adgroup_contexts"]
    matched_rows = scan["matched_rows"]
    err_details = scan["warnings"]
    scanned_keyword_count = int(scan["scanned_keyword_count"])
    matched_count = int(scan["matched_count"])
    preview_token = hashlib.sha256(f"{cid}|{search_scope}|{'/'.join(campaign_ids)}|{'/'.join(adgroup_ids)}|{search_text}|{exclude_text}|{'1' if exact_match else '0'}|{matched_count}".encode('utf-8')).hexdigest()[:20]
    scope_label = "선택 캠페인 기준" if search_scope == "selected_campaigns" else ("선택 그룹 기준" if search_scope == "selected_adgroups" else "계정 전체 기준")
    return jsonify({
        "ok": True,
        "message": f"[{scope_label}]\n" + _build_powerlink_keyword_search_message(search_text, exact_match, scan, row_preview_limit=30, exclude_text=exclude_text),
        "search_text": search_text,
        "search_groups": scan.get("search_groups") or _parse_keyword_search_groups(search_text),
        "exact_match": bool(exact_match),
        "exclude_text": exclude_text,
        "matched_count": matched_count,
        "scanned_keyword_count": scanned_keyword_count,
        "total_powerlink_campaign_count": len(powerlink_campaigns),
        "total_powerlink_adgroup_count": len(adgroup_contexts),
        "rows": matched_rows[:100],
        "warnings": err_details[:10],
        "preview_token": preview_token,
        "search_scope": search_scope,
        "selected_campaign_count": len(campaign_ids) if search_scope == "selected_campaigns" else 0,
        "selected_adgroup_count": len(adgroup_ids) if search_scope == "selected_adgroups" else 0,
    })
def update_keyword_bids_by_search():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    search_text = str(d.get("search_text") or d.get("keyword_query") or "").strip()
    exact_match = _boolish(d.get("exact_match"), False)
    exclude_text = str(d.get("exclude_text") or d.get("exclude_keyword_query") or "").strip()
    search_scope = str(d.get("search_scope") or "account").strip().lower()
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
    target_bid = _normalize_bid_amt(d.get("bid_amt"))
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not search_text:
        return jsonify({"error": "검색할 단어를 입력해주세요."}), 400
    if target_bid is None:
        return jsonify({"error": "적용할 입찰가를 올바르게 입력해주세요."}), 400
    if search_scope not in {"account", "selected_campaigns", "selected_adgroups"}:
        search_scope = "account"
    if search_scope == "selected_campaigns" and not campaign_ids:
        return jsonify({"error": "선택 캠페인 조회를 사용하려면 좌측에서 캠페인을 체크해주세요."}), 400
    if search_scope == "selected_adgroups" and not adgroup_ids:
        return jsonify({"error": "선택 그룹내 조회를 사용하려면 좌측에서 광고그룹을 체크해주세요."}), 400
    scan = _scan_powerlink_keywords_by_search(
        api_key,
        secret_key,
        cid,
        search_text,
        exact_match=exact_match,
        campaign_ids=(campaign_ids if search_scope == "selected_campaigns" else None),
        adgroup_ids=(adgroup_ids if search_scope == "selected_adgroups" else None),
        exclude_text=exclude_text,
    )
    powerlink_campaigns = scan["powerlink_campaigns"]
    adgroup_contexts = scan["adgroup_contexts"]
    matched_rows = scan["matched_rows"]
    base_payload = scan["update_payload"]
    updated_adgroup_ids = scan["updated_adgroup_ids"]
    err_details = list(scan["warnings"])
    scanned_keyword_count = int(scan["scanned_keyword_count"])
    matched_count = int(scan["matched_count"])
    scope_label = "선택 캠페인 기준" if search_scope == "selected_campaigns" else ("선택 그룹 기준" if search_scope == "selected_adgroups" else "계정 전체 기준")
    if not powerlink_campaigns:
        return jsonify({"ok": True, "message": f"[{scope_label}] 현재 계정에 파워링크 캠페인이 없습니다.", "matched_count": 0, "updated_count": 0, "skipped_count": 0, "fail_count": 0, "rows": [], "updated_adgroup_ids": [], "search_scope": search_scope})
    if not adgroup_contexts:
        return jsonify({"ok": True, "message": f"[{scope_label}] 현재 계정에서 조회 가능한 파워링크 광고그룹이 없습니다.", "matched_count": 0, "updated_count": 0, "skipped_count": 0, "fail_count": 0, "rows": [], "warnings": err_details[:10], "updated_adgroup_ids": [], "search_scope": search_scope})
    update_payload: List[Dict[str, Any]] = []
    skipped_count = 0
    bid_by_id = {str(row.get("ncc_keyword_id") or "").strip(): row for row in matched_rows}
    for item in (base_payload or []):
        item2 = copy.deepcopy(item)
        item2["useGroupBidAmt"] = False
        item2["bidAmt"] = int(target_bid)
        item_id = str(item2.get("nccKeywordId") or "").strip()
        row = bid_by_id.get(item_id) or {}
        current_bid = _normalize_bid_amt(row.get("current_bid")) or _normalize_bid_amt(item2.get("bidAmt")) or 70
        current_use_group = bool(row.get("current_use_group"))
        if current_bid == target_bid and not current_use_group:
            skipped_count += 1
            continue
        update_payload.append(item2)
    updated_count = 0
    fail_count = 0
    single_fallback_jobs: List[Dict[str, Any]] = []
    for i in range(0, len(update_payload), 100):
        batch = update_payload[i:i + 100]
        if not batch:
            continue
        r_put = _do_req("PUT", api_key, secret_key, cid, "/ncc/keywords", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=batch)
        if r_put.status_code in [200, 201]:
            updated_count += len(batch)
        else:
            single_fallback_jobs.extend(batch)
    if single_fallback_jobs:
        fb_workers = min(max(BID_IO_WORKERS, 10), max(1, len(single_fallback_jobs)))
        with ThreadPoolExecutor(max_workers=fb_workers) as fb_ex:
            fb_future_map = {
                fb_ex.submit(_do_req, "PUT", api_key, secret_key, cid, f"/ncc/keywords/{item['nccKeywordId']}", {"fields": "bidAmt,useGroupBidAmt"}, item): item
                for item in single_fallback_jobs
            }
            for fb_fut in as_completed(fb_future_map):
                item = fb_future_map[fb_fut]
                try:
                    r_single = fb_fut.result()
                except Exception as exc:
                    fail_count += 1
                    if len(err_details) < 10:
                        err_details.append(f"[{item.get('keyword', '알수없음')}] 키워드 입찰가 변경 실패: {exc}")
                    continue
                if r_single.status_code in [200, 201]:
                    updated_count += 1
                else:
                    fail_count += 1
                    if len(err_details) < 10:
                        err_details.append(f"[{item.get('keyword', '알수없음')}] 키워드 입찰가 변경 실패: {r_single.text}")
    mode_label = "완전일치" if exact_match else "부분일치"
    lines = [
        f"검색어 기준 파워링크 키워드 입찰가 변경 완료! ({mode_label})",
        f"범위: {scope_label}",
        f"검색어: {search_text}",
        (f"제외어: {exclude_text}" if str(exclude_text or "").strip() else "제외어: 없음"),
        f"대상 입찰가: {int(target_bid):,}원",
        f"조회한 파워링크 캠페인: {len(powerlink_campaigns)}개 | 광고그룹: {len(adgroup_contexts)}개 | 키워드 스캔: {scanned_keyword_count}개",
        f"검색 일치 키워드: {matched_count}개 | 변경 성공: {updated_count}개 | 유지: {skipped_count}개 | 실패: {fail_count}개",
    ]
    if matched_rows:
        lines.append("\n[일치 키워드 예시]")
        for row in matched_rows[:20]:
            current_label = f"{int(row.get('current_bid') or 0):,}원"
            if row.get("current_use_group"):
                current_label += " (그룹입찰가 사용)"
            matched_term_label = str(row.get('matched_terms_text') or '')
            if matched_term_label:
                matched_term_label = f" | 매칭: {matched_term_label}"
            lines.append(f"- {row.get('campaign_name')} > {row.get('adgroup_name')} > {row.get('keyword')}{matched_term_label} | 현재 {current_label}")
    if err_details:
        lines.append("\n[상세 내역]")
        lines.extend(err_details[:10])
    _cache_invalidate(api_key, secret_key, cid)
    return jsonify({
        "ok": True,
        "message": "\n".join(lines),
        "search_text": search_text,
        "exact_match": bool(exact_match),
        "exclude_text": exclude_text,
        "search_scope": search_scope,
        "target_bid": int(target_bid),
        "matched_count": int(matched_count),
        "updated_count": int(updated_count),
        "skipped_count": int(skipped_count),
        "fail_count": int(fail_count),
        "scanned_keyword_count": int(scanned_keyword_count),
        "total_powerlink_campaign_count": len(powerlink_campaigns),
        "total_powerlink_adgroup_count": len(adgroup_contexts),
        "rows": matched_rows[:100],
        "warnings": err_details[:10],
        "updated_adgroup_ids": _unique_keep_order(updated_adgroup_ids),
        "selected_campaign_count": len(campaign_ids) if search_scope == "selected_campaigns" else 0,
        "selected_adgroup_count": len(adgroup_ids) if search_scope == "selected_adgroups" else 0,
    })
def preview_keyword_bid_weights_by_search():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    search_text = str(d.get("search_text") or d.get("keyword_query") or "").strip()
    exact_match = _boolish(d.get("exact_match"), False)
    search_scope = str(d.get("search_scope") or "account").strip().lower()
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not search_text:
        return jsonify({"error": "검색할 단어를 입력해주세요."}), 400
    scan = _scan_powerlink_keywords_by_search(api_key, secret_key, cid, search_text, exact_match=exact_match)
    powerlink_campaigns = scan["powerlink_campaigns"]
    adgroup_contexts = scan["adgroup_contexts"]
    matched_rows = scan["matched_rows"]
    err_details = scan["warnings"]
    scanned_keyword_count = int(scan["scanned_keyword_count"])
    matched_count = int(scan["matched_count"])
    mode_label = "완전일치" if exact_match else "부분일치"
    lines = [
        f"검색어 기준 파워링크 키워드 입찰가중치 조회 완료! ({mode_label})",
        f"검색어: {search_text}",
        f"조회한 파워링크 캠페인: {len(powerlink_campaigns)}개 | 광고그룹: {len(adgroup_contexts)}개 | 키워드 스캔: {scanned_keyword_count}개",
        f"검색 일치 키워드: {matched_count}개",
    ]
    if matched_rows:
        lines.append("\n[일치 키워드 예시]")
        for row in matched_rows[:30]:
            current_label = f"{int(row.get('current_bid_weight') or 100):,}%"
            matched_term_label = str(row.get('matched_terms_text') or '')
            if matched_term_label:
                matched_term_label = f" | 매칭: {matched_term_label}"
            lines.append(f"- {row.get('campaign_name')} > {row.get('adgroup_name')} > {row.get('keyword')}{matched_term_label} | 현재 입찰가중치 {current_label}")
    if err_details:
        lines.append("\n[상세 내역]")
        lines.extend(err_details[:10])
    preview_token = hashlib.sha256(f"BW|{cid}|{search_text}|{'1' if exact_match else '0'}|{matched_count}".encode('utf-8')).hexdigest()[:20]
    return jsonify({
        "ok": True,
        "message": "\n".join(lines),
        "search_text": search_text,
        "exact_match": bool(exact_match),
        "matched_count": matched_count,
        "scanned_keyword_count": scanned_keyword_count,
        "total_powerlink_campaign_count": len(powerlink_campaigns),
        "total_powerlink_adgroup_count": len(adgroup_contexts),
        "rows": matched_rows[:100],
        "warnings": err_details[:10],
        "preview_token": preview_token,
    })
def update_keyword_bid_weights_by_search():
    d = request.json or {}
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    search_text = str(d.get("search_text") or d.get("keyword_query") or "").strip()
    exact_match = _boolish(d.get("exact_match"), False)
    search_scope = str(d.get("search_scope") or "account").strip().lower()
    campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
    target_weight = _normalize_bid_weight(d.get("bid_weight"))
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not search_text:
        return jsonify({"error": "검색할 단어를 입력해주세요."}), 400
    if target_weight is None:
        return jsonify({"error": "적용할 입찰가중치(%)를 올바르게 입력해주세요. (10~500)"}), 400
    scan = _scan_powerlink_keywords_by_search(api_key, secret_key, cid, search_text, exact_match=exact_match)
    powerlink_campaigns = scan["powerlink_campaigns"]
    adgroup_contexts = scan["adgroup_contexts"]
    matched_rows = scan["matched_rows"]
    base_payload = scan["update_payload"]
    updated_adgroup_ids = scan["updated_adgroup_ids"]
    err_details = list(scan["warnings"])
    scanned_keyword_count = int(scan["scanned_keyword_count"])
    matched_count = int(scan["matched_count"])
    if not powerlink_campaigns:
        return jsonify({"ok": True, "message": "현재 계정에 파워링크 캠페인이 없습니다.", "matched_count": 0, "updated_count": 0, "skipped_count": 0, "fail_count": 0, "rows": [], "updated_adgroup_ids": []})
    if not adgroup_contexts:
        return jsonify({"ok": True, "message": "현재 계정에서 조회 가능한 파워링크 광고그룹이 없습니다.", "matched_count": 0, "updated_count": 0, "skipped_count": 0, "fail_count": 0, "rows": [], "warnings": err_details[:10], "updated_adgroup_ids": []})
    update_payload: List[Dict[str, Any]] = []
    skipped_count = 0
    row_by_id = {str(row.get("ncc_keyword_id") or "").strip(): row for row in matched_rows}
    for item in (base_payload or []):
        item2 = copy.deepcopy(item)
        item2["bidWeight"] = int(target_weight)
        item_id = str(item2.get("nccKeywordId") or "").strip()
        row = row_by_id.get(item_id) or {}
        current_weight = _normalize_bid_weight(row.get("current_bid_weight")) or 100
        if current_weight == target_weight:
            skipped_count += 1
            continue
        update_payload.append(item2)
    updated_count = 0
    fail_count = 0
    single_fallback_jobs: List[Dict[str, Any]] = []
    for i in range(0, len(update_payload), 100):
        batch = update_payload[i:i + 100]
        if not batch:
            continue
        r_put = _do_req("PUT", api_key, secret_key, cid, "/ncc/keywords", params={"fields": "bidWeight"}, json_body=batch)
        if r_put.status_code in [200, 201]:
            updated_count += len(batch)
        else:
            single_fallback_jobs.extend(batch)
    if single_fallback_jobs:
        fb_workers = min(max(BID_IO_WORKERS, 10), max(1, len(single_fallback_jobs)))
        with ThreadPoolExecutor(max_workers=fb_workers) as fb_ex:
            fb_future_map = {
                fb_ex.submit(_do_req, "PUT", api_key, secret_key, cid, f"/ncc/keywords/{item['nccKeywordId']}", {"fields": "bidWeight"}, item): item
                for item in single_fallback_jobs
            }
            for fb_fut in as_completed(fb_future_map):
                item = fb_future_map[fb_fut]
                try:
                    r_single = fb_fut.result()
                except Exception as exc:
                    fail_count += 1
                    if len(err_details) < 10:
                        err_details.append(f"[{item.get('keyword', '알수없음')}] 키워드 입찰가중치 변경 실패: {exc}")
                    continue
                if r_single.status_code in [200, 201]:
                    updated_count += 1
                else:
                    fail_count += 1
                    if len(err_details) < 10:
                        err_details.append(f"[{item.get('keyword', '알수없음')}] 키워드 입찰가중치 변경 실패: {r_single.text}")
    mode_label = "완전일치" if exact_match else "부분일치"
    lines = [
        f"검색어 기준 파워링크 키워드 입찰가중치 변경 완료! ({mode_label})",
        f"검색어: {search_text}",
        f"대상 입찰가중치: {int(target_weight):,}%",
        f"조회한 파워링크 캠페인: {len(powerlink_campaigns)}개 | 광고그룹: {len(adgroup_contexts)}개 | 키워드 스캔: {scanned_keyword_count}개",
        f"검색 일치 키워드: {matched_count}개 | 변경 성공: {updated_count}개 | 유지: {skipped_count}개 | 실패: {fail_count}개",
    ]
    if matched_rows:
        lines.append("\n[일치 키워드 예시]")
        for row in matched_rows[:20]:
            current_label = f"{int(row.get('current_bid_weight') or 100):,}%"
            matched_term_label = str(row.get('matched_terms_text') or '')
            if matched_term_label:
                matched_term_label = f" | 매칭: {matched_term_label}"
            lines.append(f"- {row.get('campaign_name')} > {row.get('adgroup_name')} > {row.get('keyword')}{matched_term_label} | 현재 입찰가중치 {current_label}")
    if err_details:
        lines.append("\n[상세 내역]")
        lines.extend(err_details[:10])
    _cache_invalidate(api_key, secret_key, cid)
    return jsonify({
        "ok": True,
        "message": "\n".join(lines),
        "search_text": search_text,
        "exact_match": bool(exact_match),
        "target_weight": int(target_weight),
        "matched_count": int(matched_count),
        "updated_count": int(updated_count),
        "skipped_count": int(skipped_count),
        "fail_count": int(fail_count),
        "scanned_keyword_count": int(scanned_keyword_count),
        "total_powerlink_campaign_count": len(powerlink_campaigns),
        "total_powerlink_adgroup_count": len(adgroup_contexts),
        "rows": matched_rows[:100],
        "warnings": err_details[:10],
        "updated_adgroup_ids": _unique_keep_order(updated_adgroup_ids),
    })
def update_keyword_bids():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type, entity_ids = d.get("entity_type"), d.get("entity_ids", [])
    bid_amt = int(d.get("bid_amt", 70))
    if not entity_ids:
        return jsonify({"error": "대상이 없습니다."}), 400
    adgroup_contexts, resolve_warnings = _resolve_adgroup_contexts(api_key, secret_key, cid, entity_type, entity_ids)
    if not adgroup_contexts:
        return jsonify({"error": "대상 광고그룹을 찾지 못했습니다."}), 400
    adgroup_contexts, name_filter_info = _filter_adgroup_contexts_by_name_conditions(adgroup_contexts, d)
    if not adgroup_contexts:
        lines = ["광고그룹명 조건 필터 적용 후 대상 광고그룹이 없습니다."]
        lines.extend(_adgroup_name_filter_summary(name_filter_info))
        return jsonify({"ok": True, "message": "\n".join(lines)}), 200
    use_group_bid = bid_amt == 0
    target_bid = _normalize_bid_amt(bid_amt if bid_amt else 70) or 70
    kw_success = kw_fail = kw_skipped = 0
    ad_success = ad_fail = ad_skipped = 0
    err_details: List[str] = list(resolve_warnings)
    keyword_groups = 0
    shopping_groups = 0
    cleanup_keys = ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']
    keyword_contexts = [ctx for ctx in adgroup_contexts if not _adgroup_uses_ad_level_bid(ctx.get("adgroup_type"))]
    shopping_contexts = [ctx for ctx in adgroup_contexts if _adgroup_uses_ad_level_bid(ctx.get("adgroup_type"))]
    keyword_groups = len(keyword_contexts)
    shopping_groups = len(shopping_contexts)
    if keyword_contexts:
        max_workers = min(BID_IO_WORKERS, max(1, len(keyword_contexts)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {
                ex.submit(_do_req, "GET", api_key, secret_key, cid, "/ncc/keywords", {"nccAdgroupId": str(ctx.get("adgroup_id") or "").strip()}, None): ctx
                for ctx in keyword_contexts if str(ctx.get("adgroup_id") or "").strip()
            }
            single_fallback_jobs: List[Dict[str, Any]] = []
            for fut in as_completed(future_map):
                ctx = future_map[fut]
                adg_id = str(ctx.get("adgroup_id") or "").strip()
                adgroup_bid = ctx.get("adgroup_bid")
                try:
                    r_kw = fut.result()
                except Exception as exc:
                    kw_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 키워드 조회 실패: {exc}")
                    continue
                if r_kw.status_code != 200:
                    kw_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 키워드 조회 실패: {r_kw.text}")
                    continue
                kws = r_kw.json() or []
                update_payload = []
                for kw in kws:
                    item = copy.deepcopy(kw)
                    current_use_group = bool(kw.get("useGroupBidAmt"))
                    effective_current_bid = _resolve_effective_bid(kw.get("bidAmt"), current_use_group, adgroup_bid)
                    raw_keyword_bid = _normalize_bid_amt(kw.get("bidAmt")) or 70
                    should_skip = current_use_group if use_group_bid else ((effective_current_bid == target_bid) and (not current_use_group))
                    if should_skip:
                        kw_skipped += 1
                        continue
                    item["useGroupBidAmt"] = bool(use_group_bid)
                    item["bidAmt"] = raw_keyword_bid if use_group_bid else target_bid
                    for k in cleanup_keys:
                        item.pop(k, None)
                    update_payload.append(item)
                for i in range(0, len(update_payload), 100):
                    batch = update_payload[i:i + 100]
                    if not batch:
                        continue
                    r_put = _do_req("PUT", api_key, secret_key, cid, "/ncc/keywords", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=batch)
                    if r_put.status_code in [200, 201]:
                        kw_success += len(batch)
                    else:
                        for item in batch:
                            single_fallback_jobs.append(item)
            if single_fallback_jobs:
                fallback_workers = min(BID_IO_WORKERS, max(1, len(single_fallback_jobs)))
                with ThreadPoolExecutor(max_workers=fallback_workers) as fb_ex:
                    fb_future_map = {
                        fb_ex.submit(_do_req, "PUT", api_key, secret_key, cid, f"/ncc/keywords/{item['nccKeywordId']}", {"fields": "bidAmt,useGroupBidAmt"}, item): item
                        for item in single_fallback_jobs
                    }
                    for fb_fut in as_completed(fb_future_map):
                        item = fb_future_map[fb_fut]
                        try:
                            r_single = fb_fut.result()
                        except Exception as exc:
                            kw_fail += 1
                            if len(err_details) < 10:
                                err_details.append(f"[{item.get('keyword', '알수없음')}] 키워드 입찰가 변경 실패: {exc}")
                            continue
                        if r_single.status_code in [200, 201]:
                            kw_success += 1
                        else:
                            kw_fail += 1
                            if len(err_details) < 10:
                                err_details.append(f"[{item.get('keyword', '알수없음')}] 키워드 입찰가 변경 실패: {r_single.text}")
    if shopping_contexts:
        max_workers = min(BID_IO_WORKERS, max(1, len(shopping_contexts)))
        put_jobs: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {
                ex.submit(_fetch_ads, api_key, secret_key, cid, str(ctx.get("adgroup_id") or "").strip()): ctx
                for ctx in shopping_contexts if str(ctx.get("adgroup_id") or "").strip()
            }
            for fut in as_completed(future_map):
                ctx = future_map[fut]
                adg_id = str(ctx.get("adgroup_id") or "").strip()
                adgroup_bid = ctx.get("adgroup_bid")
                try:
                    res_ads, ads = fut.result()
                except Exception as exc:
                    ad_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 소재 조회 실패: {exc}")
                    continue
                if res_ads.status_code != 200:
                    ad_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 소재 조회 실패: {res_ads.text}")
                    continue
                for ad in (ads or []):
                    if not _ad_item_has_bid_attr(ad):
                        continue
                    ad_attr = _extract_ad_attr(ad)
                    current_use_group = bool(ad_attr.get("useGroupBidAmt"))
                    effective_current_bid = _resolve_effective_bid(ad_attr.get("bidAmt"), current_use_group, adgroup_bid)
                    raw_item_bid = _normalize_bid_amt(ad_attr.get("bidAmt")) or 70
                    should_skip = current_use_group if use_group_bid else ((effective_current_bid == target_bid) and (not current_use_group))
                    if should_skip:
                        ad_skipped += 1
                        continue
                    item = copy.deepcopy(ad)
                    new_attr = _extract_ad_attr(item)
                    new_attr["useGroupBidAmt"] = bool(use_group_bid)
                    new_attr["bidAmt"] = raw_item_bid if use_group_bid else target_bid
                    item["adAttr"] = new_attr
                    put_jobs.append(item)
        if put_jobs:
            put_workers = min(BID_IO_WORKERS, max(1, len(put_jobs)))
            with ThreadPoolExecutor(max_workers=put_workers) as put_ex:
                put_future_map = {put_ex.submit(_put_single_ad_with_ad_attr, api_key, secret_key, cid, item): item for item in put_jobs}
                for put_fut in as_completed(put_future_map):
                    item = put_future_map[put_fut]
                    try:
                        r_put = put_fut.result()
                    except Exception as exc:
                        ad_fail += 1
                        if len(err_details) < 10:
                            name = str((((item.get("ad") or {}).get("productName")) if isinstance(item.get("ad"), dict) else "") or _extract_ad_id(item) or "알수없음")
                            err_details.append(f"[{name}] 소재 입찰가 변경 실패: {exc}")
                        continue
                    if r_put.status_code in [200, 201]:
                        ad_success += 1
                    else:
                        ad_fail += 1
                        if len(err_details) < 10:
                            name = str((((item.get("ad") or {}).get("productName")) if isinstance(item.get("ad"), dict) else "") or _extract_ad_id(item) or "알수없음")
                            err_details.append(f"[{name}] 소재 입찰가 변경 실패: {r_put.text}")
    label = "그룹 입찰가 사용 전환" if use_group_bid else f"고정 입찰가 {target_bid:,}원 적용"
    lines = [
        f"입찰가 변경 완료! ({label})",
        f"파워링크/키워드 처리 광고그룹: {keyword_groups}개 | 쇼핑/소재 처리 광고그룹: {shopping_groups}개",
        f"키워드 변경 성공: {kw_success}개 / 실패: {kw_fail}개 / 유지: {kw_skipped}개",
        f"쇼핑 소재 변경 성공: {ad_success}개 / 실패: {ad_fail}개 / 유지: {ad_skipped}개",
    ]
    lines.extend(_adgroup_name_filter_summary(name_filter_info))
    if err_details:
        lines.append("\n[상세 내역]")
        lines.extend(err_details[:10])
    return jsonify({"ok": True, "message": "\n".join(lines)})
def update_bid_mode_by_scope():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = str(d.get("entity_type") or "adgroup").strip().lower()
    entity_ids = d.get("entity_ids") or []
    bid_mode = str(d.get("bid_mode") or "").strip().lower()
    if bid_mode not in {"group", "individual"}:
        return jsonify({"error": "입찰가 설정 방식을 확인해주세요."}), 400
    if not entity_ids:
        return jsonify({"error": "대상이 없습니다."}), 400
    adgroup_contexts, resolve_warnings = _resolve_adgroup_contexts(api_key, secret_key, cid, entity_type, entity_ids)
    if not adgroup_contexts:
        return jsonify({"error": "대상 광고그룹을 찾지 못했습니다."}), 400
    adgroup_contexts, name_filter_info = _filter_adgroup_contexts_by_name_conditions(adgroup_contexts, d)
    if not adgroup_contexts:
        lines = ["광고그룹명 조건 필터 적용 후 대상 광고그룹이 없습니다."]
        lines.extend(_adgroup_name_filter_summary(name_filter_info))
        return jsonify({"ok": True, "message": "\n".join(lines)}), 200

    target_use_group = bid_mode == "group"
    keyword_contexts = [ctx for ctx in adgroup_contexts if not _adgroup_uses_ad_level_bid(ctx.get("adgroup_type"))]
    shopping_contexts = [ctx for ctx in adgroup_contexts if _adgroup_uses_ad_level_bid(ctx.get("adgroup_type"))]
    keyword_groups = len(keyword_contexts)
    shopping_groups = len(shopping_contexts)
    kw_success = kw_fail = kw_skipped = 0
    ad_success = ad_fail = ad_skipped = 0
    err_details: List[str] = list(resolve_warnings)
    cleanup_keys = ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']

    if keyword_contexts:
        max_workers = min(BID_IO_WORKERS, max(1, len(keyword_contexts)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {
                ex.submit(_do_req, "GET", api_key, secret_key, cid, "/ncc/keywords", {"nccAdgroupId": str(ctx.get("adgroup_id") or "").strip()}, None): ctx
                for ctx in keyword_contexts if str(ctx.get("adgroup_id") or "").strip()
            }
            single_fallback_jobs: List[Dict[str, Any]] = []
            for fut in as_completed(future_map):
                ctx = future_map[fut]
                adg_id = str(ctx.get("adgroup_id") or "").strip()
                adgroup_bid = ctx.get("adgroup_bid")
                try:
                    r_kw = fut.result()
                except Exception as exc:
                    kw_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 키워드 조회 실패: {exc}")
                    continue
                if r_kw.status_code != 200:
                    kw_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 키워드 조회 실패: {r_kw.text}")
                    continue
                kws = r_kw.json() or []
                update_payload = []
                for kw in kws:
                    item = copy.deepcopy(kw)
                    current_use_group = bool(kw.get("useGroupBidAmt"))
                    raw_keyword_bid = _normalize_bid_amt(kw.get("bidAmt")) or 70
                    effective_current_bid = _resolve_effective_bid(kw.get("bidAmt"), current_use_group, adgroup_bid) or raw_keyword_bid
                    target_bid = raw_keyword_bid if target_use_group else effective_current_bid
                    should_skip = current_use_group == target_use_group
                    if should_skip:
                        kw_skipped += 1
                        continue
                    item["useGroupBidAmt"] = bool(target_use_group)
                    item["bidAmt"] = target_bid
                    for k in cleanup_keys:
                        item.pop(k, None)
                    update_payload.append(item)
                for i in range(0, len(update_payload), 100):
                    batch = update_payload[i:i + 100]
                    if not batch:
                        continue
                    r_put = _do_req("PUT", api_key, secret_key, cid, "/ncc/keywords", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=batch)
                    if r_put.status_code in [200, 201]:
                        kw_success += len(batch)
                    else:
                        single_fallback_jobs.extend(batch)
            if single_fallback_jobs:
                fallback_workers = min(BID_IO_WORKERS, max(1, len(single_fallback_jobs)))
                with ThreadPoolExecutor(max_workers=fallback_workers) as fb_ex:
                    fb_future_map = {
                        fb_ex.submit(_do_req, "PUT", api_key, secret_key, cid, f"/ncc/keywords/{item['nccKeywordId']}", {"fields": "bidAmt,useGroupBidAmt"}, item): item
                        for item in single_fallback_jobs
                    }
                    for fb_fut in as_completed(fb_future_map):
                        item = fb_future_map[fb_fut]
                        try:
                            r_single = fb_fut.result()
                        except Exception as exc:
                            kw_fail += 1
                            if len(err_details) < 10:
                                err_details.append(f"[{item.get('keyword', '알수없음')}] 입찰가 설정 방식 변경 실패: {exc}")
                            continue
                        if r_single.status_code in [200, 201]:
                            kw_success += 1
                        else:
                            kw_fail += 1
                            if len(err_details) < 10:
                                err_details.append(f"[{item.get('keyword', '알수없음')}] 입찰가 설정 방식 변경 실패: {r_single.text}")

    if shopping_contexts:
        max_workers = min(BID_IO_WORKERS, max(1, len(shopping_contexts)))
        put_jobs: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {
                ex.submit(_fetch_ads, api_key, secret_key, cid, str(ctx.get("adgroup_id") or "").strip()): ctx
                for ctx in shopping_contexts if str(ctx.get("adgroup_id") or "").strip()
            }
            for fut in as_completed(future_map):
                ctx = future_map[fut]
                adg_id = str(ctx.get("adgroup_id") or "").strip()
                adgroup_bid = ctx.get("adgroup_bid")
                try:
                    res_ads, ads = fut.result()
                except Exception as exc:
                    ad_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 소재 조회 실패: {exc}")
                    continue
                if res_ads.status_code != 200:
                    ad_fail += 1
                    if len(err_details) < 10:
                        err_details.append(f"[광고그룹 {adg_id}] 소재 조회 실패: {res_ads.text}")
                    continue
                for ad in (ads or []):
                    if not _ad_item_has_bid_attr(ad):
                        continue
                    ad_attr = _extract_ad_attr(ad)
                    current_use_group = bool(ad_attr.get("useGroupBidAmt"))
                    raw_item_bid = _normalize_bid_amt(ad_attr.get("bidAmt")) or 70
                    effective_current_bid = _resolve_effective_bid(ad_attr.get("bidAmt"), current_use_group, adgroup_bid) or raw_item_bid
                    target_bid = raw_item_bid if target_use_group else effective_current_bid
                    if current_use_group == target_use_group:
                        ad_skipped += 1
                        continue
                    item = copy.deepcopy(ad)
                    new_attr = _extract_ad_attr(item)
                    new_attr["useGroupBidAmt"] = bool(target_use_group)
                    new_attr["bidAmt"] = target_bid
                    item["adAttr"] = new_attr
                    put_jobs.append(item)
        if put_jobs:
            put_workers = min(BID_IO_WORKERS, max(1, len(put_jobs)))
            with ThreadPoolExecutor(max_workers=put_workers) as put_ex:
                put_future_map = {put_ex.submit(_put_single_ad_with_ad_attr, api_key, secret_key, cid, item): item for item in put_jobs}
                for put_fut in as_completed(put_future_map):
                    item = put_future_map[put_fut]
                    try:
                        r_put = put_fut.result()
                    except Exception as exc:
                        ad_fail += 1
                        if len(err_details) < 10:
                            name = str((((item.get("ad") or {}).get("productName")) if isinstance(item.get("ad"), dict) else "") or _extract_ad_id(item) or "알수없음")
                            err_details.append(f"[{name}] 입찰가 설정 방식 변경 실패: {exc}")
                        continue
                    if r_put.status_code in [200, 201]:
                        ad_success += 1
                    else:
                        ad_fail += 1
                        if len(err_details) < 10:
                            name = str((((item.get("ad") or {}).get("productName")) if isinstance(item.get("ad"), dict) else "") or _extract_ad_id(item) or "알수없음")
                            err_details.append(f"[{name}] 입찰가 설정 방식 변경 실패: {r_put.text}")

    label = "그룹 입찰가 사용" if target_use_group else "개별 입찰가 사용"
    lines = [
        f"입찰가 설정 방식 변경 완료! ({label})",
        f"파워링크/키워드 처리 광고그룹: {keyword_groups}개 | 쇼핑/소재 처리 광고그룹: {shopping_groups}개",
        f"키워드 변경 성공: {kw_success}개 / 실패: {kw_fail}개 / 유지: {kw_skipped}개",
        f"쇼핑 소재 변경 성공: {ad_success}개 / 실패: {ad_fail}개 / 유지: {ad_skipped}개",
    ]
    if not target_use_group:
        lines.append("개별 입찰가 전환 시 현재 적용 중인 유효 입찰가를 각 키워드/소재의 개별 입찰가로 저장합니다.")
    lines.extend(_adgroup_name_filter_summary(name_filter_info))
    if err_details:
        lines.append("\n[상세 내역]")
        lines.extend(err_details[:10])
    return jsonify({"ok": True, "message": "\n".join(lines)})

def adjust_keyword_bids_by_threshold():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = str(d.get("entity_type") or "adgroup").strip().lower()
    entity_ids = _unique_keep_order(d.get("entity_ids") or [])
    if not entity_ids:
        return jsonify({"error": "대상이 없습니다."}), 400
    def _parse_optional_bound(value: Any, label: str) -> Optional[int]:
        if str(value or "").strip() == "":
            return None
        bid = _normalize_bid_amt(value)
        if bid is None:
            raise ValueError(f"{label} 값을 확인해주세요.")
        return bid
    def _parse_signed_step(value: Any, label: str) -> int:
        s = str(value or "").strip().replace(",", "")
        if not s:
            raise ValueError(f"{label} 값을 입력해주세요.")
        try:
            return int(float(s))
        except Exception as exc:
            raise ValueError(f"{label} 값을 확인해주세요.") from exc
    try:
        upper_bid = _parse_optional_bound(d.get("upper_bid"), "상한가")
        lower_bid = _parse_optional_bound(d.get("lower_bid"), "하한가")
        upper_delta = _parse_signed_step(d.get("upper_delta"), "상한가 조정값") if upper_bid is not None else 0
        lower_delta = _parse_signed_step(d.get("lower_delta"), "하한가 조정값") if lower_bid is not None else 0
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if upper_bid is None and lower_bid is None:
        return jsonify({"error": "상한가 또는 하한가 중 하나 이상 입력해주세요."}), 400
    if upper_bid is not None and lower_bid is not None and lower_bid >= upper_bid:
        return jsonify({"error": "하한가는 상한가보다 작아야 합니다."}), 400
    if upper_bid is not None and upper_delta == 0:
        return jsonify({"error": "상한가 조정값은 0이 될 수 없습니다."}), 400
    if lower_bid is not None and lower_delta == 0:
        return jsonify({"error": "하한가 조정값은 0이 될 수 없습니다."}), 400
    adgroup_contexts, resolve_warnings = _resolve_adgroup_contexts(api_key, secret_key, cid, entity_type, entity_ids)
    if not adgroup_contexts:
        return jsonify({"error": "대상 광고그룹을 찾지 못했습니다."}), 400
    adgroup_contexts, name_filter_info = _filter_adgroup_contexts_by_name_conditions(adgroup_contexts, d)
    if not adgroup_contexts:
        lines = ["광고그룹명 조건 필터 적용 후 대상 광고그룹이 없습니다."]
        lines.extend(_adgroup_name_filter_summary(name_filter_info))
        return jsonify({"ok": True, "message": "\n".join(lines)}), 200
    keyword_meta: Dict[str, Dict[str, Any]] = {}
    keyword_bid_map: Dict[str, int] = {}
    ad_meta: Dict[str, Dict[str, Any]] = {}
    ad_bid_map: Dict[str, int] = {}
    warnings: List[str] = list(resolve_warnings)
    keyword_count = 0
    shopping_ad_count = 0
    upper_hit = 0
    lower_hit = 0
    unchanged_preview = 0
    keyword_groups = 0
    shopping_groups = 0
    for ctx in adgroup_contexts:
        adg_id = str(ctx.get("adgroup_id") or "").strip()
        adgroup_type = str(ctx.get("adgroup_type") or "").upper()
        adgroup_bid = ctx.get("adgroup_bid")
        if _adgroup_uses_ad_level_bid(adgroup_type):
            shopping_groups += 1
            res_ads, ads = _fetch_ads(api_key, secret_key, cid, adg_id)
            if res_ads.status_code != 200:
                warnings.append(f"[광고그룹 {adg_id}] 소재 조회 실패: {res_ads.text}")
                continue
            for ad in (ads or []):
                if not _ad_item_has_bid_attr(ad):
                    continue
                shopping_ad_count += 1
                ad_id = _extract_ad_id(ad)
                if not ad_id:
                    continue
                ad_attr = _extract_ad_attr(ad)
                current_use_group = bool(ad_attr.get("useGroupBidAmt"))
                effective_current_bid = _resolve_effective_bid(ad_attr.get("bidAmt"), current_use_group, adgroup_bid)
                if effective_current_bid is None:
                    continue
                ad_name = str(((ad.get("ad") or {}).get("productName") if isinstance(ad.get("ad"), dict) else "") or ad_id)
                ad_meta[ad_id] = {
                    "current_bid": effective_current_bid,
                    "use_group_bid": current_use_group,
                    "name": ad_name,
                    "adgroup_id": adg_id,
                }
                target_bid = None
                if upper_bid is not None and effective_current_bid >= upper_bid:
                    upper_hit += 1
                    target_bid = _normalize_bid_amt(upper_bid + upper_delta)
                elif lower_bid is not None and effective_current_bid <= lower_bid:
                    lower_hit += 1
                    target_bid = _normalize_bid_amt(lower_bid + lower_delta)
                if target_bid is None:
                    continue
                if target_bid == effective_current_bid and (not current_use_group):
                    unchanged_preview += 1
                    continue
                ad_bid_map[ad_id] = target_bid
        else:
            keyword_groups += 1
            r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id})
            if r_kw.status_code != 200:
                warnings.append(f"[광고그룹 {adg_id}] 키워드 조회 실패: {r_kw.text}")
                continue
            kws = r_kw.json() or []
            keyword_count += len(kws)
            for kw in kws:
                kid = str(kw.get("nccKeywordId") or "").strip()
                if not kid:
                    continue
                current_use_group = bool(kw.get("useGroupBidAmt"))
                effective_current_bid = _resolve_effective_bid(kw.get("bidAmt"), current_use_group, adgroup_bid)
                if effective_current_bid is None:
                    continue
                keyword_meta[kid] = {
                    "current_bid": effective_current_bid,
                    "use_group_bid": current_use_group,
                    "keyword": kw.get("keyword"),
                    "adgroup_id": adg_id,
                }
                target_bid = None
                if upper_bid is not None and effective_current_bid >= upper_bid:
                    upper_hit += 1
                    target_bid = _normalize_bid_amt(upper_bid + upper_delta)
                elif lower_bid is not None and effective_current_bid <= lower_bid:
                    lower_hit += 1
                    target_bid = _normalize_bid_amt(lower_bid + lower_delta)
                if target_bid is None:
                    continue
                if target_bid == effective_current_bid and (not current_use_group):
                    unchanged_preview += 1
                    continue
                keyword_bid_map[kid] = target_bid
    if not keyword_bid_map and not ad_bid_map:
        lines = [
            "조건에 맞는 키워드/소재가 없습니다.",
            f"파워링크 키워드 조회: {keyword_count}개 / 쇼핑 소재 조회: {shopping_ad_count}개 / 상한 조건 매칭: {upper_hit}개 / 하한 조건 매칭: {lower_hit}개",
        ]
        if warnings:
            lines.append("\n[참고]")
            lines.extend(warnings[:10])
        return jsonify({"ok": True, "message": "\n".join(lines)})
    kw_success = kw_fail = kw_skipped = 0
    ad_success = ad_fail = ad_skipped = 0
    err_details: List[str] = []
    if keyword_bid_map:
        kw_success, kw_fail, kw_skipped, kw_err_details = _apply_keyword_bid_map(
            api_key, secret_key, cid, [str(x.get("adgroup_id") or "") for x in adgroup_contexts if not _adgroup_uses_ad_level_bid(x.get("adgroup_type"))], keyword_bid_map, keyword_meta=keyword_meta
        )
        err_details.extend(kw_err_details)
    if ad_bid_map:
        ad_success, ad_fail, ad_skipped, ad_err_details = _apply_ad_bid_map(
            api_key, secret_key, cid, adgroup_contexts, ad_bid_map, ad_meta=ad_meta
        )
        err_details.extend(ad_err_details)
    stats = _calc_bid_stats(list(keyword_bid_map.values()) + list(ad_bid_map.values()))
    lines = [
        "상/하한 기준 입찰가 조정 완료!",
        f"파워링크/키워드 처리 광고그룹: {keyword_groups}개 | 쇼핑/소재 처리 광고그룹: {shopping_groups}개",
        f"파워링크 키워드 조회: {keyword_count}개 / 쇼핑 소재 조회: {shopping_ad_count}개 / 실제 변경 대상: {len(keyword_bid_map) + len(ad_bid_map)}개 / 동일해서 유지 예상: {unchanged_preview}개",
        f"상한 조건 매칭: {upper_hit}개 / 하한 조건 매칭: {lower_hit}개",
        f"키워드 변경 성공: {kw_success}개 / 실패: {kw_fail}개 / 유지/생략: {kw_skipped}개",
        f"쇼핑 소재 변경 성공: {ad_success}개 / 실패: {ad_fail}개 / 유지/생략: {ad_skipped}개",
    ]
    if stats.get("min") is not None:
        lines.append(f"변경 후 입찰가 범위: 최소 {stats['min']:,}원 · 중앙 {stats['median']:,}원 · 최대 {stats['max']:,}원")
    rule_bits = []
    if upper_bid is not None:
        upper_target = _normalize_bid_amt(upper_bid + upper_delta)
        rule_bits.append(f"상한 {upper_bid:,}원 이상 → {upper_bid:,}원 기준 {upper_delta:+,}원 = {upper_target:,}원")
    if lower_bid is not None:
        lower_target = _normalize_bid_amt(lower_bid + lower_delta)
        rule_bits.append(f"하한 {lower_bid:,}원 이하 → {lower_bid:,}원 기준 {lower_delta:+,}원 = {lower_target:,}원")
    if rule_bits:
        lines.append("적용 규칙: " + " / ".join(rule_bits))
    lines.extend(_adgroup_name_filter_summary(name_filter_info))
    detail_lines = warnings + err_details
    if detail_lines:
        lines.append("\n[상세 내역]")
        lines.extend(detail_lines[:10])
    return jsonify({
        "ok": True,
        "message": "\n".join(lines),
        "stats": {
            "keyword_count": keyword_count,
            "shopping_ad_count": shopping_ad_count,
            "upper_hit": upper_hit,
            "lower_hit": lower_hit,
            "targets": len(keyword_bid_map) + len(ad_bid_map),
            "success": kw_success + ad_success,
            "fail": kw_fail + ad_fail,
            "unchanged_preview": unchanged_preview,
            "skipped": kw_skipped + ad_skipped,
        },
    })

def _keyword_avg_position_by_search_common(d: Dict[str, Any], preview_only: bool):
    api_key = str(d.get("api_key") or "").strip()
    secret_key = str(d.get("secret_key") or "").strip()
    cid = str(d.get("customer_id") or "").strip()
    search_text = str(d.get("search_text") or d.get("keyword_query") or "").strip()
    exclude_text = str(d.get("exclude_text") or d.get("keyword_exclude") or "").strip()
    exact_match = _boolish(d.get("exact_match"), False)
    search_scope = str(d.get("search_scope") or "account").strip().lower()
    campaign_ids = _unique_keep_order([str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()])
    adgroup_ids = _unique_keep_order([str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()])
    device = str(d.get("device") or "PC").strip().upper()
    try:
        position = int(d.get("position") or 1)
    except Exception:
        position = 1
    include_paused = _boolish(d.get("include_paused"), False)
    include_pending = _boolish(d.get("include_pending"), False)
    if "exclude_paused" in d:
        include_paused = not _boolish(d.get("exclude_paused"), True)
    if "exclude_pending" in d:
        include_pending = not _boolish(d.get("exclude_pending"), True)
    max_bid_raw = d.get("max_bid")
    max_bid = None
    if str(max_bid_raw or "").strip() != "":
        max_bid = _normalize_bid_amt(max_bid_raw)
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not search_text:
        return jsonify({"error": "검색어를 입력해주세요."}), 400
    if device not in {"PC", "MOBILE"}:
        return jsonify({"error": "디바이스는 PC 또는 MOBILE 이어야 합니다."}), 400
    max_position = 10 if device == "PC" else 5
    if position < 1 or position > max_position:
        return jsonify({"error": f"목표 평균순위는 {device} 기준 1~{max_position} 사이로 입력해주세요."}), 400

    scope_campaign_ids = campaign_ids if search_scope == "campaign" else None
    scope_adgroup_ids = adgroup_ids if search_scope == "adgroup" else None
    if search_scope == "campaign" and not scope_campaign_ids:
        return jsonify({"error": "좌측에서 캠페인을 선택해주세요."}), 400
    if search_scope == "adgroup" and not scope_adgroup_ids:
        return jsonify({"error": "좌측에서 광고그룹을 선택해주세요."}), 400

    scan = _scan_powerlink_keywords_by_search(
        api_key, secret_key, cid, search_text,
        exact_match=exact_match,
        campaign_ids=scope_campaign_ids,
        adgroup_ids=scope_adgroup_ids,
        exclude_text=exclude_text,
    )
    matched_rows = scan.get("matched_rows") or []
    if not matched_rows:
        scope_label = "선택 캠페인" if search_scope == "campaign" else ("선택 광고그룹" if search_scope == "adgroup" else "전체 계정")
        return jsonify({
            "ok": True,
            "preview": bool(preview_only),
            "message": f"검색어 기준 평균순위 입찰가 대상 키워드가 없습니다.\n범위: {scope_label}\n검색어: {search_text}",
            "matched_count": 0,
            "estimated": 0,
            "changed": 0,
            "updated_count": 0,
            "fail_count": 0,
            "rows": [],
        })

    keyword_ids: List[str] = []
    keyword_meta: Dict[str, Dict[str, Any]] = {}
    keyword_adgroup_ids: List[str] = []
    skipped_paused = 0
    skipped_pending = 0
    skipped_invalid = 0
    for row in matched_rows:
        kid = str(row.get("ncc_keyword_id") or "").strip()
        if not kid:
            skipped_invalid += 1
            continue
        if (not include_paused) and row.get("enabled") is False:
            skipped_paused += 1
            continue
        check_row = {
            "status": row.get("status"),
            "inspectStatus": row.get("inspect_status"),
            "delFlag": row.get("del_flag"),
        }
        ok, reason = _is_keyword_editable_for_avg_position(check_row, include_paused=include_paused, include_pending=include_pending)
        if not ok:
            if str(reason).startswith("상태:"):
                skipped_paused += 1
            elif str(reason).startswith("검수:"):
                skipped_pending += 1
            else:
                skipped_invalid += 1
            continue
        keyword_ids.append(kid)
        keyword_adgroup_ids.append(str(row.get("adgroup_id") or "").strip())
        keyword_meta[kid] = {
            "keyword": str(row.get("keyword") or ""),
            "current_bid": row.get("current_bid"),
            "use_group_bid": bool(row.get("current_use_group")),
            "adgroup_id": str(row.get("adgroup_id") or "").strip(),
        }
    keyword_ids = _unique_keep_order(keyword_ids)
    keyword_adgroup_ids = _unique_keep_order(keyword_adgroup_ids)
    if not keyword_ids:
        lines = [
            "검색어 기준 파워링크 평균순위 입찰가 대상이 없습니다.",
            f"검색어: {search_text}",
            f"검색 일치 키워드: {len(matched_rows)}개",
        ]
        if skipped_paused:
            lines.append(f"중지 상태 제외: {skipped_paused}개")
        if skipped_pending:
            lines.append(f"검수보류/비승인 제외: {skipped_pending}개")
        if skipped_invalid:
            lines.append(f"기타 제외: {skipped_invalid}개")
        return jsonify({"ok": True, "preview": bool(preview_only), "message": "\n".join(lines), "matched_count": len(matched_rows), "estimated": 0, "changed": 0, "updated_count": 0, "fail_count": 0, "rows": []})

    estimated_map, estimate_warnings = _estimate_keyword_bids_by_avg_position(api_key, secret_key, cid, keyword_ids, device, position, max_bid=max_bid, keyword_meta=keyword_meta)
    if not estimated_map:
        msg = f"{device} {position}위 평균순위 추정값을 받지 못했습니다."
        warnings = list(scan.get("warnings") or []) + list(estimate_warnings or [])
        if warnings:
            msg += "\n" + "\n".join(warnings[:5])
        return jsonify({"error": msg}), 400

    changed_bids: List[int] = []
    unchanged_cnt = 0
    preview_rows: List[Dict[str, Any]] = []
    for row in matched_rows:
        kid = str(row.get("ncc_keyword_id") or "").strip()
        if kid not in estimated_map:
            continue
        current_bid = _normalize_bid_amt(row.get("current_bid")) or 70
        current_use_group = bool(row.get("current_use_group"))
        new_bid = int(estimated_map[kid])
        will_change = (current_bid != new_bid) or current_use_group
        if will_change:
            changed_bids.append(new_bid)
        else:
            unchanged_cnt += 1
        if len(preview_rows) < 40:
            preview_rows.append({
                "campaign_name": row.get("campaign_name"),
                "adgroup_name": row.get("adgroup_name"),
                "keyword": row.get("keyword"),
                "keyword_id": kid,
                "current_bid": current_bid,
                "new_bid": new_bid,
                "use_group_bid": current_use_group,
                "matched_terms_text": row.get("matched_terms_text"),
                "will_change": will_change,
            })
    stats = _calc_bid_stats(list(estimated_map.values()))
    mode_label = "완전일치" if exact_match else "부분일치"
    scope_label = "선택 캠페인" if search_scope == "campaign" else ("선택 광고그룹" if search_scope == "adgroup" else "전체 계정")
    lines = [
        f"검색어 기준 파워링크 평균순위 {device} {position}위 입찰가 {'미리보기' if preview_only else '적용'} 완료 ({mode_label})",
        f"범위: {scope_label}",
        f"검색어: {search_text}",
    ]
    if exclude_text:
        lines.append(f"제외 검색어: {exclude_text}")
    lines.extend([
        f"검색 일치 키워드: {len(matched_rows)}개 / 추정 성공: {len(estimated_map)}개",
        f"변경 예정: {len(changed_bids)}개 / 동일해서 유지: {unchanged_cnt}개",
    ])
    if stats.get("min") is not None:
        lines.append(f"예상 입찰가 범위: 최소 {stats['min']:,}원 · 중앙 {stats['median']:,}원 · 최대 {stats['max']:,}원")
    if max_bid is not None:
        lines.append(f"최대 입찰가 상한 적용: {max_bid:,}원")
    if skipped_paused:
        lines.append(f"중지 상태 제외: {skipped_paused}개")
    if skipped_pending:
        lines.append(f"검수보류/비승인 제외: {skipped_pending}개")
    if skipped_invalid:
        lines.append(f"기타 제외: {skipped_invalid}개")
    if preview_rows:
        lines.append("\n[대상 예시]")
        for row in preview_rows[:25]:
            cur = f"{int(row.get('current_bid') or 0):,}원"
            if row.get("use_group_bid"):
                cur += " (그룹입찰가 사용)"
            matched_label = f" | 매칭: {row.get('matched_terms_text')}" if row.get("matched_terms_text") else ""
            lines.append(f"- {row.get('campaign_name')} > {row.get('adgroup_name')} > {row.get('keyword')}{matched_label} | 현재 {cur} → 예상 {int(row.get('new_bid') or 0):,}원")
    warnings = list(scan.get("warnings") or []) + list(estimate_warnings or [])
    if warnings:
        lines.append("\n[참고]")
        lines.extend(warnings[:5])

    if preview_only:
        token_src = f"{cid}|{search_text}|{exclude_text}|{exact_match}|{search_scope}|{','.join(scope_campaign_ids or [])}|{','.join(scope_adgroup_ids or [])}|{device}|{position}|{max_bid or ''}|{len(estimated_map)}|{len(changed_bids)}"
        preview_token = hashlib.sha256(token_src.encode("utf-8")).hexdigest()[:20]
        return jsonify({
            "ok": True,
            "preview": True,
            "message": "\n".join(lines),
            "preview_token": preview_token,
            "matched_count": len(matched_rows),
            "estimated": len(estimated_map),
            "changed": len(changed_bids),
            "unchanged": unchanged_cnt,
            "stats": stats,
            "rows": preview_rows,
            "skipped_paused": skipped_paused,
            "skipped_pending": skipped_pending,
        })

    success_cnt, fail_cnt, skipped_cnt, err_details = _apply_keyword_bid_map(api_key, secret_key, cid, keyword_adgroup_ids, estimated_map, keyword_meta=keyword_meta)
    lines.append(f"\n변경 성공: {success_cnt}개 / 실패: {fail_cnt}개 / 유지/생략: {skipped_cnt}개")
    if err_details:
        lines.append("\n[실패 상세]")
        lines.extend(err_details[:5])
    return jsonify({
        "ok": True,
        "preview": False,
        "message": "\n".join(lines),
        "matched_count": len(matched_rows),
        "estimated": len(estimated_map),
        "changed": len(changed_bids),
        "updated_count": success_cnt,
        "fail_count": fail_cnt,
        "skipped_count": skipped_cnt,
        "updated_adgroup_ids": keyword_adgroup_ids,
        "rows": preview_rows,
        "warnings": warnings[:10],
    }), (200 if fail_cnt == 0 else 207)


def preview_keyword_avg_position_by_search():
    return _keyword_avg_position_by_search_common(request.json or {}, preview_only=True)


def update_keyword_avg_position_by_search():
    return _keyword_avg_position_by_search_common(request.json or {}, preview_only=False)


def update_keyword_bids_avg_position():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = str(d.get("entity_type") or "adgroup").strip()
    entity_ids = d.get("entity_ids", []) or []
    device = str(d.get("device") or "PC").upper()
    position = int(d.get("position") or 1)
    preview_only = bool(d.get("preview_only"))
    avg_target_type = str(d.get("avg_target_type") or d.get("campaign_type") or d.get("target_type") or "all").strip().lower()
    avg_target_alias = {
        "powerlink": "powerlink",
        "web_site": "powerlink",
        "website": "powerlink",
        "파워링크": "powerlink",
        "shopping": "shopping",
        "shop": "shopping",
        "shopping_search": "shopping",
        "쇼핑검색": "shopping",
        "all": "all",
        "": "all",
    }
    avg_target_type = avg_target_alias.get(avg_target_type, avg_target_type)
    if avg_target_type not in {"powerlink", "shopping", "all"}:
        return jsonify({"error": "avg_target_type은 powerlink 또는 shopping 이어야 합니다."}), 400
    include_paused = _boolish(d.get("include_paused"), False)
    include_pending = _boolish(d.get("include_pending"), False)
    if "exclude_paused" in d:
        include_paused = not _boolish(d.get("exclude_paused"), True)
    if "exclude_pending" in d:
        include_pending = not _boolish(d.get("exclude_pending"), True)
    max_bid_raw = d.get("max_bid")
    max_bid = None
    if str(max_bid_raw or "").strip() != "":
        try:
            max_bid = _normalize_bid_amt(max_bid_raw)
        except Exception:
            max_bid = None
    if device not in {"PC", "MOBILE"}:
        return jsonify({"error": "device는 PC 또는 MOBILE 이어야 합니다."}), 400
    max_position = 10 if device == "PC" else 5
    if position < 1 or position > max_position:
        return jsonify({"error": f"목표 평균순위는 {device} 기준 1~{max_position} 사이로 입력해주세요."}), 400
    if not entity_ids:
        return jsonify({"error": "대상이 없습니다."}), 400
    adgroup_contexts, resolve_warnings = _resolve_adgroup_contexts(api_key, secret_key, cid, entity_type, entity_ids)
    if not adgroup_contexts:
        msg = "적용 가능한 광고그룹을 찾지 못했습니다."
        if resolve_warnings:
            msg += "\n" + "\n".join(resolve_warnings[:5])
        return jsonify({"error": msg}), 400
    adgroup_contexts, name_filter_info = _filter_adgroup_contexts_by_name_conditions(adgroup_contexts, d)
    if not adgroup_contexts:
        lines = ["광고그룹명 조건 필터 적용 후 대상 광고그룹이 없습니다."]
        lines.extend(_adgroup_name_filter_summary(name_filter_info))
        return jsonify({"ok": True, "message": "\n".join(lines)}), 200
    warnings: List[str] = list(resolve_warnings)
    keyword_adgroup_ids: List[str] = []
    shopping_contexts: List[Dict[str, Any]] = []
    skipped_other_groups = 0
    skipped_unselected_type = 0
    for ctx in adgroup_contexts:
        adgroup_type = str(ctx.get("adgroup_type") or "").upper()
        is_shopping = _adgroup_uses_ad_level_bid(adgroup_type)
        is_powerlink = (adgroup_type == "WEB_SITE") or (not adgroup_type and not is_shopping)

        if avg_target_type == "powerlink":
            if is_powerlink:
                keyword_adgroup_ids.append(str(ctx.get("adgroup_id") or "").strip())
            else:
                skipped_unselected_type += 1
            continue

        if avg_target_type == "shopping":
            if is_shopping:
                shopping_contexts.append(ctx)
            else:
                skipped_unselected_type += 1
            continue

        # 호환용(all): 기존처럼 파워링크+쇼핑을 모두 처리하되, 기타 유형만 제외합니다.
        if is_shopping:
            shopping_contexts.append(ctx)
        elif is_powerlink:
            keyword_adgroup_ids.append(str(ctx.get("adgroup_id") or "").strip())
        else:
            skipped_other_groups += 1
    keyword_ids: List[str] = []
    keyword_meta: Dict[str, Dict[str, Any]] = {}
    keyword_count = 0
    keyword_fetch_fail = 0
    skipped_keyword_paused = 0
    skipped_keyword_pending = 0
    for adg_id in keyword_adgroup_ids:
        if not adg_id:
            continue
        r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id})
        if r_kw.status_code != 200:
            keyword_fetch_fail += 1
            if len(warnings) < 10:
                warnings.append(f"광고그룹 {adg_id} 키워드 조회 실패: {r_kw.text}")
            continue
        rows = r_kw.json() or []
        keyword_count += len(rows)
        for kw in rows:
            kid = str(kw.get("nccKeywordId") or "").strip()
            if not kid:
                continue
            ok, reason = _is_keyword_editable_for_avg_position(kw, include_paused=include_paused, include_pending=include_pending)
            if not ok:
                if str(reason).startswith("상태:"):
                    skipped_keyword_paused += 1
                else:
                    skipped_keyword_pending += 1
                continue
            keyword_ids.append(kid)
            keyword_meta[kid] = {
                "keyword": str(kw.get("keyword") or ""),
                "current_bid": kw.get("bidAmt"),
                "use_group_bid": bool(kw.get("useGroupBidAmt")),
                "adgroup_id": adg_id,
            }
    keyword_ids = _unique_keep_order(keyword_ids)
    ad_ids: List[str] = []
    ad_meta: Dict[str, Dict[str, Any]] = {}
    shopping_ad_count = 0
    shopping_fetch_fail = 0
    skipped_ad_paused = 0
    skipped_ad_pending = 0
    for ctx in shopping_contexts:
        adg_id = str(ctx.get("adgroup_id") or "").strip()
        adgroup_bid = ctx.get("adgroup_bid")
        res_ads, ads = _fetch_ads(api_key, secret_key, cid, adg_id)
        if res_ads.status_code != 200:
            shopping_fetch_fail += 1
            if len(warnings) < 10:
                warnings.append(f"광고그룹 {adg_id} 소재 조회 실패: {res_ads.text}")
            continue
        for ad in (ads or []):
            if not _ad_item_has_bid_attr(ad):
                continue
            shopping_ad_count += 1
            ad_id = _extract_ad_id(ad)
            if not ad_id:
                continue
            ok, reason = _is_ad_editable_for_avg_position(ad, include_paused=include_paused, include_pending=include_pending)
            if not ok:
                if str(reason).startswith("상태:"):
                    skipped_ad_paused += 1
                else:
                    skipped_ad_pending += 1
                continue
            ad_attr = _extract_ad_attr(ad)
            effective_current_bid = _resolve_effective_bid(ad_attr.get("bidAmt"), bool(ad_attr.get("useGroupBidAmt")), adgroup_bid)
            ad_name = str(((ad.get("ad") or {}).get("productName") if isinstance(ad.get("ad"), dict) else "") or ad_id)
            ad_ids.append(ad_id)
            ad_meta[ad_id] = {
                "name": ad_name,
                "current_bid": effective_current_bid,
                "use_group_bid": bool(ad_attr.get("useGroupBidAmt")),
                "adgroup_id": adg_id,
            }
    ad_ids = _unique_keep_order(ad_ids)
    if not keyword_ids and not ad_ids:
        type_label = "파워링크 키워드" if avg_target_type == "powerlink" else ("쇼핑검색 소재" if avg_target_type == "shopping" else "키워드/소재")
        msg = f"평균순위 추정에 사용할 활성 {type_label}가 없습니다."
        if skipped_unselected_type:
            msg += f" 선택한 광고유형이 아닌 광고그룹 {skipped_unselected_type}개는 제외되었습니다."
        return jsonify({"error": msg}), 400
    estimated_keyword_bid_map: Dict[str, int] = {}
    estimated_ad_bid_map: Dict[str, int] = {}
    if keyword_ids:
        estimated_keyword_bid_map, estimate_warnings = _estimate_keyword_bids_by_avg_position(api_key, secret_key, cid, keyword_ids, device, position, max_bid=max_bid, keyword_meta=keyword_meta)
        warnings.extend(estimate_warnings)
    if ad_ids:
        estimated_ad_bid_map, ad_estimate_warnings = _estimate_ad_bids_by_avg_position(api_key, secret_key, cid, ad_ids, device, position, max_bid=max_bid)
        warnings.extend(ad_estimate_warnings)
    if not estimated_keyword_bid_map and not estimated_ad_bid_map:
        msg = f"{device} {position}위 평균순위 추정값을 받지 못했습니다."
        if warnings:
            msg += "\n" + "\n".join(warnings[:5])
        return jsonify({"error": msg}), 400
    preview_rows = []
    changed_bids: List[int] = []
    unchanged_cnt = 0
    for kid in keyword_ids:
        if kid not in estimated_keyword_bid_map:
            continue
        meta = keyword_meta.get(kid, {})
        current_bid = _normalize_bid_amt(meta.get("current_bid"))
        new_bid = estimated_keyword_bid_map[kid]
        will_change = (current_bid != new_bid) or bool(meta.get("use_group_bid"))
        if will_change:
            changed_bids.append(new_bid)
        else:
            unchanged_cnt += 1
        if len(preview_rows) < 30:
            preview_rows.append({
                "target_type": "keyword",
                "name": meta.get("keyword") or kid,
                "keyword": meta.get("keyword") or kid,
                "keyword_id": kid,
                "current_bid": current_bid,
                "new_bid": new_bid,
                "use_group_bid": bool(meta.get("use_group_bid")),
                "will_change": will_change,
            })
    for ad_id in ad_ids:
        if ad_id not in estimated_ad_bid_map:
            continue
        meta = ad_meta.get(ad_id, {})
        current_bid = _normalize_bid_amt(meta.get("current_bid"))
        new_bid = estimated_ad_bid_map[ad_id]
        will_change = (current_bid != new_bid) or bool(meta.get("use_group_bid"))
        if will_change:
            changed_bids.append(new_bid)
        else:
            unchanged_cnt += 1
        if len(preview_rows) < 30:
            preview_rows.append({
                "target_type": "ad",
                "name": meta.get("name") or ad_id,
                "ad_id": ad_id,
                "current_bid": current_bid,
                "new_bid": new_bid,
                "use_group_bid": bool(meta.get("use_group_bid")),
                "will_change": will_change,
            })
    def _calc_stats(values: List[int]):
        if not values:
            return {"min": None, "max": None, "median": None}
        vals = sorted(values)
        n = len(vals)
        if n % 2:
            median = vals[n // 2]
        else:
            median = int(round((vals[n // 2 - 1] + vals[n // 2]) / 2.0 / 10.0) * 10)
        return {"min": vals[0], "max": vals[-1], "median": median}
    stats = _calc_stats(list(estimated_keyword_bid_map.values()) + list(estimated_ad_bid_map.values()))
    if preview_only:
        type_label = "파워링크" if avg_target_type == "powerlink" else ("쇼핑검색" if avg_target_type == "shopping" else "전체")
        lines = [
            f"{type_label} · {device} 평균순위 {position}위 기준 변경사항 확인 완료",
            f"파워링크 추정 성공: {len(estimated_keyword_bid_map)}개 / 적용 대상 키워드: {len(keyword_ids)}개",
            f"쇼핑 추정 성공: {len(estimated_ad_bid_map)}개 / 적용 대상 소재: {len(ad_ids)}개",
            f"변경 예정: {len(changed_bids)}개 / 동일해서 유지: {unchanged_cnt}개",
        ]
        if stats["min"] is not None:
            lines.append(f"예상 입찰가 범위: 최소 {stats['min']:,}원 · 중앙 {stats['median']:,}원 · 최대 {stats['max']:,}원")
        if max_bid is not None:
            lines.append(f"최대 입찰가 상한 적용: {max_bid:,}원")
        if skipped_keyword_paused or skipped_ad_paused:
            lines.append(f"중지 상태 제외: {skipped_keyword_paused + skipped_ad_paused}개")
        if skipped_keyword_pending or skipped_ad_pending:
            lines.append(f"검수보류/비승인 제외: {skipped_keyword_pending + skipped_ad_pending}개")
        if skipped_unselected_type:
            lines.append(f"선택한 광고유형이 아닌 광고그룹 제외: {skipped_unselected_type}개")
        if skipped_other_groups:
            lines.append(f"기타 광고그룹 제외: {skipped_other_groups}개")
        lines.extend(_adgroup_name_filter_summary(name_filter_info))
        for msg in warnings[:5]:
            lines.append(msg)
        return jsonify({
            "ok": True,
            "preview": True,
            "message": "\n".join(lines),
            "stats": stats,
            "rows": preview_rows,
            "estimated": len(estimated_keyword_bid_map) + len(estimated_ad_bid_map),
            "estimated_keywords": len(estimated_keyword_bid_map),
            "estimated_ads": len(estimated_ad_bid_map),
            "changed": len(changed_bids),
            "unchanged": unchanged_cnt,
            "skipped_paused": skipped_keyword_paused + skipped_ad_paused,
            "skipped_pending": skipped_keyword_pending + skipped_ad_pending,
        })
    kw_success = kw_fail = kw_skipped = 0
    ad_success = ad_fail = ad_skipped = 0
    err_details: List[str] = []
    if estimated_keyword_bid_map:
        kw_success, kw_fail, kw_skipped, kw_err_details = _apply_keyword_bid_map(api_key, secret_key, cid, keyword_adgroup_ids, estimated_keyword_bid_map, keyword_meta=keyword_meta)
        err_details.extend(kw_err_details)
    if estimated_ad_bid_map:
        ad_success, ad_fail, ad_skipped, ad_err_details = _apply_ad_bid_map(api_key, secret_key, cid, shopping_contexts, estimated_ad_bid_map, ad_meta=ad_meta)
        err_details.extend(ad_err_details)
    type_label = "파워링크" if avg_target_type == "powerlink" else ("쇼핑검색" if avg_target_type == "shopping" else "전체")
    lines = [
        f"{type_label} · {device} 평균순위 {position}위 기준 입찰가 적용 완료!",
        f"파워링크 광고그룹: {len(keyword_adgroup_ids)}개 / 쇼핑 광고그룹: {len(shopping_contexts)}개",
        f"파워링크 추정 성공: {len(estimated_keyword_bid_map)}개 / 전체 조회 키워드: {keyword_count}개",
        f"쇼핑 추정 성공: {len(estimated_ad_bid_map)}개 / 전체 조회 소재: {shopping_ad_count}개",
        f"키워드 변경 성공: {kw_success}개 / 실패: {kw_fail}개 / 유지/생략: {kw_skipped}개",
        f"쇼핑 소재 변경 성공: {ad_success}개 / 실패: {ad_fail}개 / 유지/생략: {ad_skipped}개",
        f"동일해서 유지: {unchanged_cnt}개",
    ]
    if stats["min"] is not None:
        lines.append(f"예상 입찰가 범위: 최소 {stats['min']:,}원 · 중앙 {stats['median']:,}원 · 최대 {stats['max']:,}원")
    if max_bid is not None:
        lines.append(f"최대 입찰가 상한 적용: {max_bid:,}원")
    if skipped_unselected_type:
        lines.append(f"선택한 광고유형이 아닌 광고그룹 건너뜀: {skipped_unselected_type}개")
    if skipped_other_groups:
        lines.append(f"기타 광고그룹 건너뜀: {skipped_other_groups}개")
    if keyword_fetch_fail:
        lines.append(f"키워드 조회 실패 광고그룹: {keyword_fetch_fail}개")
    if shopping_fetch_fail:
        lines.append(f"소재 조회 실패 광고그룹: {shopping_fetch_fail}개")
    if skipped_keyword_paused or skipped_ad_paused:
        lines.append(f"중지 상태 제외: {skipped_keyword_paused + skipped_ad_paused}개")
    if skipped_keyword_pending or skipped_ad_pending:
        lines.append(f"검수보류/비승인 제외: {skipped_keyword_pending + skipped_ad_pending}개")
    if kw_skipped + ad_skipped:
        lines.append(f"추정값 없음/변경 생략: {kw_skipped + ad_skipped}개")
    lines.extend(_adgroup_name_filter_summary(name_filter_info))
    for msg in (warnings[:5] + err_details[:5]):
        lines.append(msg)
    return jsonify({
        "ok": True,
        "message": "\n".join(lines),
        "estimated": len(estimated_keyword_bid_map) + len(estimated_ad_bid_map),
        "estimated_keywords": len(estimated_keyword_bid_map),
        "estimated_ads": len(estimated_ad_bid_map),
        "success": kw_success + ad_success,
        "fail": kw_fail + ad_fail,
        "skipped": kw_skipped + ad_skipped,
        "unchanged": unchanged_cnt,
        "stats": stats,
    })
def _delete_payload_rows(api_key: str, secret_key: str, cid: str, entity_type: str, rows: List[Dict[str, Any]]):
    def _delete_one(idx: int, row: Dict[str, Any]):
        name = f"{idx}행"
        if entity_type in {"campaign", "adgroup", "keyword", "ad", "ad_extension"}:
            key_map = {
                "campaign": ["nccCampaignId", "캠페인ID", "campaign_id"],
                "adgroup": ["nccAdgroupId", "광고그룹ID", "adgroup_id"],
                "keyword": ["nccKeywordId", "키워드ID", "keyword_id"],
                "ad": ["nccAdId", "소재ID", "ad_id"],
                "ad_extension": ["adExtensionId", "nccAdExtensionId", "확장소재ID", "확장소재 ID", "ad_extension_id"],
            }
            entity_id = ""
            for k in key_map[entity_type]:
                entity_id = str(row.get(k) or "").strip()
                if entity_id:
                    break
            res = _delete_entity_by_id(api_key, secret_key, cid, entity_type, entity_id)
            name = entity_id or name
            ok = res.status_code in [200, 201, 204]
            return idx, ok, _result_item(idx, ok, name, "삭제 완료" if ok else getattr(res, 'text', '삭제 실패'))
        elif entity_type == "restricted_keyword":
            adgroup_id = str(row.get("nccAdgroupId") or row.get("광고그룹ID") or row.get("adgroup_id") or "").strip()
            keyword = str(row.get("keyword") or row.get("제외키워드") or "").strip()
            name = keyword or name
            if not adgroup_id or not keyword:
                return idx, False, _result_item(idx, False, name, "광고그룹ID / 제외키워드는 필수입니다.")
            res = _do_req("DELETE", api_key, secret_key, cid, "/ncc/restricted-keywords", params={"nccAdgroupId": adgroup_id, "keyword": keyword})
            ok = res.status_code in [200, 201, 204]
            return idx, ok, _result_item(idx, ok, name, "삭제 완료" if ok else getattr(res, 'text', '삭제 실패'))
        return idx, False, _result_item(idx, False, name, "지원하지 않는 삭제 유형")
    indexed_rows = list(enumerate(rows, start=1))
    if not indexed_rows:
        return 0, 0, []
    results_map: Dict[int, Dict[str, Any]] = {}
    success = fail = 0
    max_workers = min(DELETE_IO_WORKERS, max(1, len(indexed_rows)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(_delete_one, idx, row): idx for idx, row in indexed_rows}
        for fut in as_completed(future_map):
            idx = future_map[fut]
            try:
                row_idx, ok, result_item = fut.result()
            except Exception as exc:
                row_idx = idx
                ok = False
                result_item = _result_item(idx, False, f"{idx}행", str(exc))
            results_map[row_idx] = result_item
            if ok:
                success += 1
            else:
                fail += 1
    results = [results_map[i] for i, _ in indexed_rows]
    return success, fail, results
def _normalize_bulk_extension_delete_type(value: Any) -> str:
    s = str(value or "").strip().upper()
    if not s or s == "ALL":
        return "ALL"
    if s == "SHOPPING_PROMO_TEXT":
        return "PROMOTION"
    if s == "쇼핑상품부가정보".upper():
        return "SHOPPING_EXTRA"
    return _normalize_extension_type(s) or s
def _bulk_extension_type_label(value: Any) -> str:
    normalized = _normalize_bulk_extension_delete_type(value)
    if normalized == "ALL":
        return "모든 확장소재"
    if str(value or "").strip().upper() == "SHOPPING_PROMO_TEXT":
        return "쇼핑 추가홍보문구"
    return AD_EXTENSION_TYPE_LABELS.get(normalized, normalized or "확장소재")
def _infer_extension_delete_type(ext_item: Dict[str, Any] | None) -> str:
    """Best-effort extension type inference for delete/export responses.

    Naver responses are not fully consistent by owner scope. Some rows expose
    `type`, others only contain an `adExtension` payload. Bulk delete must not
    miss HEADLINE / IMAGE_SUB_LINKS just because the type alias is blank.
    """
    if not isinstance(ext_item, dict):
        return ""
    for key in ("type", "adExtensionType", "extensionType", "adExtType"):
        raw = str(ext_item.get(key) or "").strip()
        if raw:
            normalized = _normalize_bulk_extension_delete_type(raw)
            if normalized and normalized != "ALL":
                return normalized
    ext = ext_item.get("adExtension")
    if isinstance(ext, list):
        def _has_image(obj: Any) -> bool:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    lk = str(k or "").lower()
                    if v and ("image" in lk or lk in {"assetid", "fileid", "nccimageid"}):
                        return True
                    if _has_image(v):
                        return True
            elif isinstance(obj, list):
                return any(_has_image(x) for x in obj)
            return False
        return "IMAGE_SUB_LINKS" if _has_image(ext) else "SUB_LINKS"
    if isinstance(ext, dict):
        keys = {str(k or "").strip() for k in ext.keys()}
        lower_keys = {k.lower() for k in keys}
        if "headline" in lower_keys or "headlinePin" in keys or "headlinepin" in lower_keys:
            return "HEADLINE"
        if {"basictext", "additionaltext"} & lower_keys:
            return "PROMOTION"
        if "description" in lower_keys:
            return "DESCRIPTION_EXTRA"
        if any(("image" in k.lower()) for k in keys):
            return "POWER_LINK_IMAGE"
        if {"businessname", "siteurl", "website"} & lower_keys:
            return "WEBSITE_INFO"
        if {"phone", "phonenumber"} & lower_keys:
            return "PHONE"
        if {"address", "location"} & lower_keys:
            return "LOCATION"
    image_ids = _extract_extension_image_ids(ext_item)
    if image_ids:
        raw = str(ext_item.get("type") or "").upper()
        if "SUB" in raw:
            return "IMAGE_SUB_LINKS"
        return "POWER_LINK_IMAGE"
    return ""


def _extension_item_matches_delete_type(ext_item: Dict[str, Any], requested_type: str) -> Tuple[bool, str]:
    target_type = _normalize_bulk_extension_delete_type(requested_type)
    resolved_type = _infer_extension_delete_type(ext_item)
    if target_type == "ALL":
        return True, resolved_type or target_type
    if not resolved_type:
        return False, ""
    if target_type == resolved_type:
        return True, resolved_type
    # 이미지형 서브링크가 SUB_LINKS 계열 alias로 내려오는 일부 응답 보정
    if target_type == "IMAGE_SUB_LINKS" and resolved_type == "SUB_LINKS" and _extract_extension_image_ids(ext_item):
        return True, "IMAGE_SUB_LINKS"
    if target_type == "SUB_LINKS" and resolved_type == "IMAGE_SUB_LINKS":
        return False, resolved_type
    return False, resolved_type


def _looks_like_shopping_ad(ad_item: Dict[str, Any] | None) -> bool:
    ad_type = str((ad_item or {}).get("type") or (ad_item or {}).get("adType") or "").upper()
    return ("SHOPPING" in ad_type) or ("CATALOG" in ad_type) or ("PRODUCT" in ad_type)
def _extract_ad_id(ad_item: Dict[str, Any] | None) -> str:
    return str((ad_item or {}).get("nccAdId") or (ad_item or {}).get("id") or (ad_item or {}).get("ownerId") or "").strip()
def _collect_target_adgroup_ids(api_key: str, secret_key: str, cid: str, parent_type: str, parent_ids: List[str]) -> Tuple[List[str], List[str]]:
    adgroup_ids: List[str] = []
    errors: List[str] = []
    seen = set()
    if parent_type == "campaign":
        normalized_campaign_ids = [str(x or "").strip() for x in (parent_ids or []) if str(x or "").strip()]
        if normalized_campaign_ids:
            max_workers = min(FAST_IO_WORKERS, max(1, len(normalized_campaign_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                future_map = {ex.submit(_fetch_adgroups, api_key, secret_key, cid, campaign_id): campaign_id for campaign_id in normalized_campaign_ids}
                ordered_rows: Dict[str, List[Dict[str, Any]]] = {}
                for fut in as_completed(future_map):
                    campaign_id = future_map[fut]
                    try:
                        res, rows = fut.result()
                    except Exception as exc:
                        errors.append(f"[캠페인 {campaign_id}] 광고그룹 조회 실패: {exc}")
                        continue
                    if res.status_code != 200:
                        errors.append(f"[캠페인 {campaign_id}] 광고그룹 조회 실패: {res.text}")
                        continue
                    ordered_rows[campaign_id] = rows or []
            for campaign_id in normalized_campaign_ids:
                for row in ordered_rows.get(campaign_id, []):
                    adgroup_id = str(row.get("id") or row.get("nccAdgroupId") or "").strip()
                    if adgroup_id and adgroup_id not in seen:
                        seen.add(adgroup_id)
                        adgroup_ids.append(adgroup_id)
    else:
        for adgroup_id in parent_ids:
            adgroup_id = str(adgroup_id or "").strip()
            if adgroup_id and adgroup_id not in seen:
                seen.add(adgroup_id)
                adgroup_ids.append(adgroup_id)
    return adgroup_ids, errors
def _collect_extension_delete_rows(api_key: str, secret_key: str, cid: str, adgroup_ids: List[str], ext_type: str, campaign_ids: List[str] | None = None) -> Tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    seen_ext_ids = set()
    requested_type = str(ext_type or "ALL").strip().upper()
    normalized_type = _normalize_bulk_extension_delete_type(requested_type)
    shopping_promo_only = requested_type == "SHOPPING_PROMO_TEXT"
    # 확장소재는 계정/생성 경로/API 응답 형태에 따라 campaign/adgroup/ad owner 기준으로 섞여 내려올 수 있다.
    # 특히 추가제목/홍보문구/추가설명이 ad owner 기준으로 존재하는 계정이 있어, 타입별로 owner 범위를 좁게 잡으면
    # "삭제할 추가제목이 없습니다"처럼 잘못 보일 수 있다. 삭제 전 조회 단계에서는 가능한 owner scope를 모두 확인하고,
    # adExtensionId 기준으로 dedupe 한다.
    include_campaign_owner = bool(campaign_ids)
    include_adgroup_owner = True
    include_ad_owner = True
    def _push_ext_items(items: List[Dict[str, Any]], owner_label: str):
        for item in (items or []):
            if not isinstance(item, dict):
                continue
            ext_id = _extract_ad_extension_id(item)
            if not ext_id or ext_id in seen_ext_ids:
                continue
            matched, resolved_type = _extension_item_matches_delete_type(item, requested_type)
            if shopping_promo_only:
                if resolved_type != "PROMOTION":
                    continue
            elif not matched:
                continue
            seen_ext_ids.add(ext_id)
            rows.append({
                "adExtensionId": ext_id,
                "_owner": owner_label,
                "_type": resolved_type or _normalize_bulk_extension_delete_type(item.get("type")) or requested_type,
                "_rawType": str(item.get("type") or ""),
            })
    if include_campaign_owner:
        seen_campaign_ids = set()
        for campaign_id in (campaign_ids or []):
            campaign_id = str(campaign_id or "").strip()
            if not campaign_id or campaign_id in seen_campaign_ids:
                continue
            seen_campaign_ids.add(campaign_id)
            res_ext, ext_items = _fetch_extensions(api_key, secret_key, cid, campaign_id)
            if res_ext.status_code == 200:
                _push_ext_items(ext_items, f"campaign:{campaign_id}")
            else:
                errors.append(f"[캠페인 {campaign_id}] 확장소재 조회 실패: {res_ext.text}")
    for adgroup_id in adgroup_ids:
        adgroup_id = str(adgroup_id or "").strip()
        if not adgroup_id:
            continue
        adgroup_obj = None
        res_detail, adgroup_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
        if res_detail.status_code != 200:
            errors.append(f"[광고그룹 {adgroup_id}] 상세 조회 실패: {res_detail.text}")
        if include_adgroup_owner:
            res_ext, ext_items = _fetch_extensions(api_key, secret_key, cid, adgroup_id)
            if res_ext.status_code == 200:
                _push_ext_items(ext_items, f"adgroup:{adgroup_id}")
            else:
                errors.append(f"[광고그룹 {adgroup_id}] 확장소재 조회 실패: {res_ext.text}")
        if include_ad_owner:
            res_ads, ads = _fetch_ads(api_key, secret_key, cid, adgroup_id)
            if res_ads.status_code != 200:
                errors.append(f"[광고그룹 {adgroup_id}] 소재 조회 실패: {res_ads.text}")
                continue
            adgroup_is_shopping = _is_shopping_adgroup(adgroup_obj)
            for ad_item in (ads or []):
                ad_id = _extract_ad_id(ad_item)
                if not ad_id:
                    continue
                # 쇼핑 외 타입도 ad owner 기준으로 확장소재가 매달린 경우가 있어 유형과 무관하게 확인한다.
                # 다만 ALL/쇼핑 계열은 기존처럼 그대로 포함되고, 파워링크 추가제목/홍보문구/추가설명도 여기서 잡힌다.
                if shopping_promo_only and not (adgroup_is_shopping or _looks_like_shopping_ad(ad_item)):
                    continue
                res_ad_ext, ad_ext_items = _fetch_extensions(api_key, secret_key, cid, ad_id)
                if res_ad_ext.status_code == 200:
                    _push_ext_items(ad_ext_items, f"ad:{ad_id}")
                else:
                    errors.append(f"[소재 {ad_id}] 확장소재 조회 실패: {res_ad_ext.text}")
    return rows, errors
def bulk_delete_by_parent():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    target_entity = str(d.get("target_entity") or "").strip()

    # v20: UI can send both checked campaign ids and checked adgroup ids at once.
    # Keep the old parent_type/parent_ids shape for backward compatibility.
    parent_type = str(d.get("parent_type") or "campaign").strip()
    parent_ids = [str(x).strip() for x in (d.get("parent_ids") or []) if str(x).strip()]
    campaign_parent_ids = [str(x).strip() for x in (d.get("campaign_ids") or []) if str(x).strip()]
    adgroup_parent_ids = [str(x).strip() for x in (d.get("adgroup_ids") or []) if str(x).strip()]
    if not campaign_parent_ids and not adgroup_parent_ids:
        if parent_type == "campaign":
            campaign_parent_ids = parent_ids
        elif parent_type == "adgroup":
            adgroup_parent_ids = parent_ids

    raw_ext_types = d.get("ext_types")
    if isinstance(raw_ext_types, list):
        ext_types = [str(x or "").strip() for x in raw_ext_types if str(x or "").strip()]
    else:
        ext_types = [str(d.get("ext_type") or "ALL").strip() or "ALL"]
    # ALL이 포함되면 다른 유형을 함께 보낼 필요가 없다.
    if any(str(x).upper() == "ALL" for x in ext_types):
        ext_types = ["ALL"]

    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if target_entity not in {"keyword", "ad", "extension"}:
        return jsonify({"error": "지원하지 않는 삭제 대상입니다."}), 400
    if not campaign_parent_ids and not adgroup_parent_ids:
        return jsonify({"error": "선택된 대상이 없습니다."}), 400

    collect_errors: List[str] = []
    adgroup_ids: List[str] = []
    seen_adgroups = set()

    if campaign_parent_ids:
        child_ids, errors = _collect_target_adgroup_ids(api_key, secret_key, cid, "campaign", campaign_parent_ids)
        collect_errors.extend(errors)
        for gid in child_ids:
            if gid and gid not in seen_adgroups:
                seen_adgroups.add(gid)
                adgroup_ids.append(gid)
    if adgroup_parent_ids:
        child_ids, errors = _collect_target_adgroup_ids(api_key, secret_key, cid, "adgroup", adgroup_parent_ids)
        collect_errors.extend(errors)
        for gid in child_ids:
            if gid and gid not in seen_adgroups:
                seen_adgroups.add(gid)
                adgroup_ids.append(gid)

    if not adgroup_ids and collect_errors and target_entity != "extension":
        return jsonify({"error": "하위 광고그룹 조회에 실패했습니다.", "details": collect_errors[:10]}), 400

    rows: List[Dict[str, Any]] = []
    entity_type = {"keyword": "keyword", "ad": "ad", "extension": "ad_extension"}[target_entity]
    seen = set()
    if target_entity == "keyword":
        if adgroup_ids:
            max_workers = min(FAST_IO_WORKERS, max(1, len(adgroup_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                future_map = {ex.submit(_fetch_keywords, api_key, secret_key, cid, adgroup_id): adgroup_id for adgroup_id in adgroup_ids}
                ordered_items: Dict[str, List[Dict[str, Any]]] = {}
                for fut in as_completed(future_map):
                    adgroup_id = future_map[fut]
                    try:
                        res, items = fut.result()
                    except Exception as exc:
                        collect_errors.append(f"[광고그룹 {adgroup_id}] 키워드 조회 실패: {exc}")
                        continue
                    if res.status_code != 200:
                        collect_errors.append(f"[광고그룹 {adgroup_id}] 키워드 조회 실패: {res.text}")
                        continue
                    ordered_items[adgroup_id] = items or []
            for adgroup_id in adgroup_ids:
                for item in ordered_items.get(adgroup_id, []):
                    keyword_id = str((item or {}).get("nccKeywordId") or "").strip()
                    if keyword_id and keyword_id not in seen:
                        seen.add(keyword_id)
                        rows.append({"nccKeywordId": keyword_id})
    elif target_entity == "ad":
        for adgroup_id in adgroup_ids:
            res, items = _fetch_ads(api_key, secret_key, cid, adgroup_id)
            if res.status_code != 200:
                collect_errors.append(f"[광고그룹 {adgroup_id}] 소재 조회 실패: {res.text}")
                continue
            for item in (items or []):
                ad_id = _extract_ad_id(item)
                if ad_id and ad_id not in seen:
                    seen.add(ad_id)
                    rows.append({"nccAdId": ad_id})
    else:
        seen_ext_ids = set()
        for ext_type in ext_types:
            part_rows, ext_errors = _collect_extension_delete_rows(
                api_key,
                secret_key,
                cid,
                adgroup_ids,
                ext_type,
                campaign_ids=campaign_parent_ids,
            )
            collect_errors.extend(ext_errors)
            for row in part_rows:
                ext_id = str(row.get("adExtensionId") or "").strip()
                if ext_id and ext_id not in seen_ext_ids:
                    seen_ext_ids.add(ext_id)
                    rows.append(row)

    if not rows:
        scope_bits = []
        if campaign_parent_ids:
            scope_bits.append(f"캠페인 {len(campaign_parent_ids)}개")
        if adgroup_parent_ids:
            scope_bits.append(f"광고그룹 {len(adgroup_parent_ids)}개")
        scope_label = " + ".join(scope_bits) or "선택 범위"
        target_label = {
            "keyword": "키워드",
            "ad": "소재",
            "extension": ", ".join(_bulk_extension_type_label(x) for x in ext_types),
        }[target_entity]
        msg = f"선택한 {scope_label} 범위에서 삭제할 {target_label}가 없습니다."
        if target_entity == "extension" and any(str(x or "").strip().upper() == "HEADLINE" for x in ext_types):
            msg += "\n(캠페인/광고그룹/소재 owner 기준으로 확장소재를 모두 조회했고, type 누락 응답은 headline 필드 기준으로도 다시 판별했습니다.)"
        if target_entity == "extension" and any(str(x or "").strip().upper() == "IMAGE_SUB_LINKS" for x in ext_types):
            msg += "\n(이미지형 서브링크는 IMAGE_SUB_LINKS 타입과 이미지 ID가 포함된 SUB_LINKS 응답까지 함께 판별했습니다.)"
        if collect_errors:
            msg += "\n" + "\n".join(collect_errors[:10])
        return jsonify({"ok": True, "total": 0, "success": 0, "fail": 0, "results": [], "message": msg})

    success, fail, results = _delete_payload_rows(api_key, secret_key, cid, entity_type, rows)
    msg_target = {
        "keyword": "키워드",
        "ad": "소재",
        "extension": ", ".join(_bulk_extension_type_label(x) for x in ext_types),
    }[target_entity]
    scope_bits = []
    if campaign_parent_ids:
        scope_bits.append(f"캠페인 {len(campaign_parent_ids)}개")
    if adgroup_parent_ids:
        scope_bits.append(f"광고그룹 {len(adgroup_parent_ids)}개")
    scope_label = " + ".join(scope_bits) or "선택 범위"
    msg = f"{msg_target} 일괄 삭제 완료 ({scope_label} / 대상 {len(rows)}건 / 성공 {success} / 실패 {fail})"
    if collect_errors:
        msg += "\n" + "\n".join(collect_errors[:10])
    return jsonify({
        "ok": True,
        "entity_type": entity_type,
        "total": len(rows),
        "success": success,
        "fail": fail,
        "results": results,
        "message": msg,
        "patch": "bulk-delete-extension-types-v20-20260429",
    })
def bulk_register():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = d.get("entity_type")
    rows = d.get("rows") or []
    raw_text = d.get("raw_text") or ""
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not rows and raw_text:
        rows = _read_table_text(raw_text)
    if not rows:
        return jsonify({"error": "등록할 데이터가 없습니다."}), 400
    handlers = {
        "campaign": _bulk_create_campaigns,
        "adgroup": _bulk_create_adgroups,
        "keyword": _bulk_create_keywords,
        "ad": _bulk_create_ads,
        "ad_extension": _bulk_create_extensions,
        "restricted_keyword": _bulk_create_restricted_keywords,
    }
    handler = handlers.get(entity_type)
    if not handler:
        return jsonify({"error": f"지원하지 않는 entity_type: {entity_type}"}), 400
    success, fail, results = handler(api_key, secret_key, cid, rows)
    return jsonify({"ok": True, "entity_type": entity_type, "total": len(rows), "success": success, "fail": fail, "results": results})
def bulk_delete():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = d.get("entity_type")
    rows = d.get("rows") or []
    raw_text = d.get("raw_text") or ""
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not rows and raw_text:
        rows = _read_table_text(raw_text)
    if not rows:
        return jsonify({"error": "삭제할 데이터가 없습니다."}), 400
    success, fail, results = _delete_payload_rows(api_key, secret_key, cid, entity_type, rows)
    return jsonify({"ok": True, "entity_type": entity_type, "total": len(rows), "success": success, "fail": fail, "results": results})
def _set_user_lock_for_entity(api_key: str, secret_key: str, cid: str, entity_type: str, entity_id: str, enabled: bool) -> Tuple[bool, str]:
    entity_id = str(entity_id or "").strip()
    if not entity_id:
        return False, "ID가 비어 있습니다."
    uri = f"/ncc/campaigns/{entity_id}" if entity_type == "campaign" else f"/ncc/adgroups/{entity_id}"
    r_get = _do_req("GET", api_key, secret_key, cid, uri)
    if r_get.status_code != 200:
        return False, f"조회 실패: {r_get.text}"
    obj = r_get.json() or {}
    obj["userLock"] = not enabled
    r_put = _do_req("PUT", api_key, secret_key, cid, uri, params={"fields": "userLock"}, json_body=obj)
    if r_put.status_code in [200, 201]:
        return True, ""
    return False, f"변경 실패: {r_put.text}"
def set_searched_powerlink_keyword_state():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    ids = d.get("ids") or []
    enabled = bool(d.get("enabled", True))
    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not isinstance(ids, list) or not ids:
        return jsonify({"error": "상태를 변경할 키워드를 선택해주세요."}), 400
    normalized_ids: List[str] = []
    seen: set[str] = set()
    for item in ids:
        kid = str(item or "").strip()
        if not kid or kid in seen:
            continue
        seen.add(kid)
        normalized_ids.append(kid)
    if not normalized_ids:
        return jsonify({"error": "유효한 키워드 ID가 없습니다."}), 400
    success = 0
    fail = 0
    details: List[str] = []
    max_workers = min(max(1, BID_IO_WORKERS), max(1, len(normalized_ids)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(_set_keyword_state, api_key, secret_key, cid, kid, enabled): kid for kid in normalized_ids}
        for fut in as_completed(future_map):
            kid = future_map[fut]
            try:
                ok, detail = fut.result()
            except Exception as exc:
                ok, detail = False, str(exc)
            if ok:
                success += 1
            else:
                fail += 1
                if detail and len(details) < 12:
                    details.append(f"[{kid}] {detail}")
    msg = f"조회 키워드 {'ON' if enabled else 'OFF'} 완료! (성공: {success} / 실패: {fail})"
    if details:
        msg += "\n" + "\n".join(details)
    _cache_invalidate(api_key, secret_key, cid)
    return jsonify({"ok": True, "message": msg, "success": success, "fail": fail, "enabled": enabled})
def set_campaign_state():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    ids = d.get("ids") or []
    enabled = bool(d.get("enabled", True))
    if not ids:
        return jsonify({"error": "선택된 캠페인이 없습니다."}), 400
    success = fail = 0
    details: List[str] = []
    child_success = child_fail = 0
    for camp_id in ids:
        camp_id = str(camp_id or "").strip()
        ok, detail = _set_user_lock_for_entity(api_key, secret_key, cid, "campaign", camp_id, enabled)
        if ok:
            success += 1
        else:
            fail += 1
            if detail and len(details) < 8:
                details.append(f"[캠페인 {camp_id}] {detail}")
            continue
        if enabled:
            r_adg, rows = _fetch_adgroups(api_key, secret_key, cid, camp_id)
            if r_adg.status_code != 200:
                if len(details) < 8:
                    details.append(f"[캠페인 {camp_id}] 하위 광고그룹 조회 실패: {r_adg.text}")
                continue
            for row in rows:
                adg_id = str(row.get("id") or row.get("nccAdgroupId") or "").strip()
                if not adg_id:
                    continue
                ok_child, child_detail = _set_user_lock_for_entity(api_key, secret_key, cid, "adgroup", adg_id, enabled)
                if ok_child:
                    child_success += 1
                else:
                    child_fail += 1
                    if child_detail and len(details) < 8:
                        details.append(f"[광고그룹 {adg_id}] {child_detail}")
    msg = f"캠페인 {'ON' if enabled else 'OFF'} 완료! (캠페인 성공: {success} / 실패: {fail})"
    if enabled:
        msg += f" · 하위 광고그룹 ON 반영: 성공 {child_success} / 실패 {child_fail}"
    if details:
        msg += "\n" + "\n".join(details)
    _cache_invalidate(d.get("api_key"), d.get("secret_key"), d.get("customer_id"))
    return jsonify({"ok": True, "message": msg})
def delete_selected():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = d.get("entity_type")
    ids = d.get("ids") or []
    rows = []
    key_name = {"campaign": "nccCampaignId", "adgroup": "nccAdgroupId", "keyword": "nccKeywordId", "ad": "nccAdId", "ad_extension": "adExtensionId"}.get(entity_type)
    if not key_name:
        return jsonify({"error": "지원하지 않는 선택삭제 유형입니다."}), 400
    for x in ids:
        rows.append({key_name: x})
    success, fail, results = _delete_payload_rows(api_key, secret_key, cid, entity_type, rows)
    return jsonify({"ok": True, "total": len(rows), "success": success, "fail": fail, "results": results})
@app.route("/sample_headers", methods=["GET"])
def sample_headers():
    entity_type = request.args.get("entity_type", "campaign")
    headers = ENTITY_SAMPLE_HEADERS.get(entity_type, [])
    sample_row = {h: "" for h in headers}
    if entity_type == "campaign":
        sample_row.update({"캠페인명": "브랜드_검색", "캠페인유형": "파워링크", "일예산사용": "예", "일예산": "50000"})
    elif entity_type == "adgroup":
        sample_row.update({"캠페인ID": "cmp-a001", "광고그룹명": "브랜드_기본", "광고그룹유형": "파워링크", "일예산사용": "예", "일예산": "10000", "입찰가": "100", "비즈채널ID": "bsn-a001-..."})
    elif entity_type == "keyword":
        sample_row.update({"광고그룹ID": "grp-a001", "키워드": "브랜드키워드", "그룹입찰가사용": "아니오", "입찰가": "120", "사용자잠금": "아니오"})
    elif entity_type == "ad":
        sample_row.update({"광고그룹ID": "grp-a001", "소재유형": "기본소재", "제목": "브랜드 공식 상담", "설명": "빠른 견적 상담 안내", "PC랜딩URL": "https://example.com", "모바일랜딩URL": "https://m.example.com", "사용자잠금": "아니오"})
    elif entity_type == "ad_extension":
        sample_row.update({"소유ID": "grp-a001", "확장소재유형": "서브링크", "원본JSON": '{"type":"SUB_LINKS","ownerId":"grp-a001","links":[{"title":"상담문의","pc":{"final":"https://example.com"},"mobile":{"final":"https://m.example.com"}}]}'})
    elif entity_type == "restricted_keyword":
        sample_row.update({"광고그룹ID": "grp-a001", "제외키워드": "무료"})
    return jsonify({"headers": headers, "sample_row": sample_row})
@app.route("/delete_sample_headers", methods=["GET"])
def delete_sample_headers():
    entity_type = request.args.get("entity_type", "campaign")
    headers = DELETE_SAMPLE_HEADERS.get(entity_type, [])
    sample_row = {h: "" for h in headers}
    if headers:
        sample_row[headers[0]] = "example-id"
    return jsonify({"headers": headers, "sample_row": sample_row})

# Stage 4~6 route split: keep existing business logic but move URL bindings into blueprints.
_REGISTRATION_SERVICE = RegistrationService({
    "create_campaign": create_campaign,
    "create_adgroup_simple": create_adgroup_simple,
    "create_keywords_simple": create_keywords_simple,
    "bulk_upload_text_ads": bulk_upload_text_ads,
    "create_text_ad_simple": create_text_ad_simple,
    "create_ad_advanced": create_ad_advanced,
    "create_extension_simple": create_extension_simple,
    "bulk_upload_headlines": bulk_upload_headlines,
    "create_shopping_ad_simple": create_shopping_ad_simple,
    "create_extension_raw": create_extension_raw,
    "create_restricted_keywords_simple": create_restricted_keywords_simple,
    "bulk_register": bulk_register,
})
app.register_blueprint(create_registration_blueprint(_REGISTRATION_SERVICE))

_CHANGE_SERVICE = ChangeService({
    "rename_adgroups_bulk": rename_adgroups_bulk,
    "update_media": update_media,
    "update_adgroup_options": update_adgroup_options,
    "update_powerlink_device_bid_weights": update_powerlink_device_bid_weights,
    "apply_target_settings_bulk": apply_target_settings_bulk,
    "update_budget": update_budget,
    "update_schedule": update_schedule,
    "update_schedule_campaign_bulk": update_schedule_campaign_bulk,
    "update_non_search_keyword_exclusion": update_non_search_keyword_exclusion,
    "preview_keyword_bids_by_search": preview_keyword_bids_by_search,
    "update_keyword_bids_by_search": update_keyword_bids_by_search,
    "preview_keyword_bid_weights_by_search": preview_keyword_bid_weights_by_search,
    "update_keyword_bid_weights_by_search": update_keyword_bid_weights_by_search,
    "update_keyword_bids": update_keyword_bids,
    "update_bid_mode_by_scope": update_bid_mode_by_scope,
    "adjust_keyword_bids_by_threshold": adjust_keyword_bids_by_threshold,
    "preview_keyword_avg_position_by_search": preview_keyword_avg_position_by_search,
    "update_keyword_avg_position_by_search": update_keyword_avg_position_by_search,
    "update_keyword_bids_avg_position": update_keyword_bids_avg_position,
    "set_searched_powerlink_keyword_state": set_searched_powerlink_keyword_state,
    "set_campaign_state": set_campaign_state,
})
app.register_blueprint(create_change_blueprint(_CHANGE_SERVICE))

_COPY_DELETE_SERVICE = CopyDeleteService({
    "copy_entities_to_adgroups": copy_entities_to_adgroups,
    "copy_campaigns": copy_campaigns,
    "copy_adgroups_to_target": copy_adgroups_to_target,
    "bulk_delete_by_parent": bulk_delete_by_parent,
    "bulk_delete": bulk_delete,
    "delete_selected": delete_selected,
})
app.register_blueprint(create_copy_delete_blueprint(_COPY_DELETE_SERVICE))

if __name__ == "__main__":
    os.makedirs(SAMPLES_DIR, exist_ok=True)
    app.run(debug=True, port=5000)
