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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Tuple, Optional
from urllib.parse import urlparse

import pandas as pd
import requests
from flask import Flask, Response, jsonify, render_template, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
SAMPLES_DIR = os.path.join(BASE_DIR, "samples")
OPENAPI_BASE_URL = "https://api.searchad.naver.com"

app = Flask(__name__, template_folder=TEMPLATES_DIR)

DAY_NUM_TO_CODE = {1: "MON", 2: "TUE", 3: "WED", 4: "THU", 5: "FRI", 6: "SAT", 7: "SUN"}
SHOPPING_AD_TYPES = {"SHOPPING_PRODUCT_AD", "CATALOG_PRODUCT_AD", "SHOPPING_BRAND_AD"}

CAMPAIGN_TYPE_LABELS = {
    "WEB_SITE": "파워링크",
    "SHOPPING": "쇼핑검색",
}
CAMPAIGN_TYPE_COLORS = {
    "WEB_SITE": "blue",
    "SHOPPING": "green",
}
ADGROUP_TYPE_LABELS = {
    "WEB_SITE": "파워링크",
    "SHOPPING": "쇼핑검색",
    "SHOPPING_BRAND": "쇼핑브랜드",
    "CATALOG": "카탈로그",
}
AD_TYPE_LABELS = {
    "TEXT_45": "기본소재(제목+설명)",
    "SHOPPING_PRODUCT_AD": "상품소재",
    "CATALOG_PRODUCT_AD": "카탈로그 상품소재",
    "SHOPPING_BRAND_AD": "쇼핑브랜드 소재",
}
AD_EXTENSION_TYPE_LABELS = {
    "HEADLINE": "추가 제목",
    "SUB_LINKS": "서브링크",
    "DESCRIPTION": "추가 설명문구",
    "DESCRIPTION_EXTRA": "설명 확장문구",
    "SHOPPING_PROMO_TEXT": "쇼핑 추가홍보문구",
    "SHOPPING_EXTRA": "쇼핑상품부가정보",
    "PHONE": "전화번호",
    "LOCATION": "위치정보",
    "PROMOTION": "프로모션",
    "PRICE_LINKS": "가격링크",
    "POWER_LINK_IMAGE": "파워링크 이미지",
    "WEBSITE_INFO": "웹사이트 정보",
    "IMAGE_SUB_LINKS": "이미지 서브링크",
}
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
    return jsonify({"error": f"서버 내부 오류: {str(e)}"}), 500


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
    msg = f"{ts}.{method.upper()}.{uri}"
    dig = hmac.new(str(secret_key).strip().encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(dig).decode()


def _open_headers(api_key: str, secret_key: str, customer_id: str, method: str, uri: str) -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "X-Timestamp": ts,
        "X-API-KEY": str(api_key).strip(),
        "X-Customer": str(customer_id).strip(),
        "X-Signature": _sig(ts, method, uri, secret_key),
        "Content-Type": "application/json; charset=UTF-8",
    }


def _make_fake_response(status_code: int, text: str):
    class FakeResponse:
        def __init__(self, status_code: int, text: str):
            self.status_code = status_code
            self.text = text
            self.content = text.encode("utf-8", errors="ignore")

        def json(self):
            try:
                return json.loads(self.text)
            except Exception:
                return {"error": self.text}

    return FakeResponse(status_code, text)


def _do_req(method, api_key, secret_key, cid, uri, params=None, json_body=None, max_retries=3):
    url = OPENAPI_BASE_URL + uri
    last_err = None
    for i in range(max_retries):
        headers = _open_headers(api_key, secret_key, cid, method, uri)
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=20)
            if r.status_code in [200, 201, 204]:
                return r
            if r.status_code == 429:
                time.sleep(1.25)
                continue
            if r.status_code == 404 and "1018" in r.text:
                time.sleep(1.0)
                continue
            return r
        except requests.exceptions.RequestException as e:
            last_err = str(e)
            time.sleep(1.25)
    return _make_fake_response(500, f"네트워크 통신 실패: {last_err or '알 수 없는 오류'}")


def _campaign_label(value: Any) -> str:
    return CAMPAIGN_TYPE_LABELS.get(str(value or "").strip(), str(value or "-"))


def _adgroup_label(value: Any) -> str:
    return ADGROUP_TYPE_LABELS.get(str(value or "").strip(), str(value or "-"))


def _ad_label(value: Any) -> str:
    return AD_TYPE_LABELS.get(str(value or "").strip(), str(value or "-"))


def _extension_label(value: Any) -> str:
    return AD_EXTENSION_TYPE_LABELS.get(str(value or "").strip(), str(value or "-"))


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
    if s == "SUB_LINK":
        return "SUB_LINKS"
    if s == "SHOPPING_PROMO_TEXT":
        return "PROMOTION"
    if s == "쇼핑상품부가정보".upper():
        return "SHOPPING_EXTRA"
    return s


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
            if isinstance(raw_json, dict):
                payload["ad"] = raw_json
            else:
                payload["ad"] = {
                    "headline": str(payload.pop("headline", payload.pop("title", "")) or "").strip(),
                    "description": str(payload.pop("description", "") or "").strip(),
                    "pc": {"final": str(payload.pop("pcFinalUrl", payload.pop("finalUrl", "")) or "").strip()},
                    "mobile": {"final": str(payload.pop("mobileFinalUrl", payload.pop("mobileUrl", payload.pop("finalUrl", ""))) or "").strip()},
                }
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
    return {
        "id": item.get("nccCampaignId") or item.get("id") or "",
        "name": item.get("name") or "",
        "campaignTp": item.get("campaignTp") or "",
        "label": _campaign_label(item.get("campaignTp")),
        "badgeColor": CAMPAIGN_TYPE_COLORS.get(item.get("campaignTp"), "gray"),
        "raw": item,
    }


def _normalize_adgroup_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": item.get("nccAdgroupId") or "",
        "name": item.get("name") or "",
        "adgroupType": item.get("adgroupType") or "",
        "label": _adgroup_label(item.get("adgroupType")),
        "pcChannelId": item.get("pcChannelId") or "",
        "mobileChannelId": item.get("mobileChannelId") or "",
        "nccCampaignId": item.get("nccCampaignId") or "",
        "raw": item,
    }


def _normalize_channel_item(item: Dict[str, Any]) -> Dict[str, Any]:
    cid = item.get("nccBusinessChannelId") or item.get("bizChannelId") or item.get("nccChannelId") or item.get("channelId") or ""
    name = item.get("name") or item.get("channelContents") or item.get("siteUrl") or cid
    return {
        "id": cid,
        "name": name,
        "channelTp": item.get("channelTp") or item.get("bizChannelType") or item.get("channelType") or "",
        "siteUrl": item.get("siteUrl") or "",
        "raw": item,
    }


def _fetch_campaigns(api_key: str, secret_key: str, cid: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/campaigns")
    if res.status_code != 200:
        return res, []
    rows = [_normalize_campaign_item(x) for x in (res.json() or [])]
    rows.sort(key=lambda x: (x["label"], x["name"]))
    return res, rows



def _fetch_campaign_detail(api_key: str, secret_key: str, cid: str, campaign_id: str):
    campaign_id = str(campaign_id or "").strip()
    if not campaign_id:
        return None
    try:
        res = _do_req("GET", api_key, secret_key, cid, f"/ncc/campaigns/{campaign_id}")
        if res.status_code == 200 and isinstance(res.json(), dict):
            return res.json()
    except Exception:
        pass
    try:
        _, rows = _fetch_campaigns(api_key, secret_key, cid)
        for row in (rows or []):
            if str(row.get("id") or "").strip() == campaign_id:
                raw = row.get("raw")
                if isinstance(raw, dict):
                    return raw
                return {"nccCampaignId": row.get("id"), "campaignTp": row.get("campaignTp"), "name": row.get("name")}
    except Exception:
        pass
    return None


def _pc_mobile_label(pc: Any, mobile: Any) -> str:
    if pc is True and mobile is False:
        return "PC"
    if pc is False and mobile is True:
        return "MOBILE"
    if pc is True and mobile is True:
        return "ALL"
    return "UNKNOWN"


def _extract_pc_mobile_flags(detail_obj: Dict[str, Any] | None, pm_target_obj: Dict[str, Any] | None = None) -> Tuple[Optional[bool], Optional[bool]]:
    detail_obj = detail_obj or {}
    pc = detail_obj.get("pcDevice")
    mobile = detail_obj.get("mobileDevice")
    if isinstance(pc, bool) and isinstance(mobile, bool):
        return pc, mobile
    target = (pm_target_obj or {}).get("target") if isinstance(pm_target_obj, dict) else None
    if isinstance(target, dict):
        pc_t = target.get("pc")
        mobile_t = target.get("mobile")
        if isinstance(pc_t, bool) and isinstance(mobile_t, bool):
            return pc_t, mobile_t
    return None, None


def _enrich_adgroup_media_row(api_key: str, secret_key: str, cid: str, row: Dict[str, Any]) -> Dict[str, Any]:
    row = copy.deepcopy(row)
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    adgroup_id = str(row.get("id") or raw.get("nccAdgroupId") or "").strip()
    if not adgroup_id:
        return row

    detail_obj: Dict[str, Any] | None = None
    pm_target_obj: Dict[str, Any] | None = None

    res_detail, detail_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
    if res_detail.status_code == 200 and isinstance(detail_obj, dict):
        merged_raw = copy.deepcopy(raw)
        merged_raw.update(detail_obj)
        raw = merged_raw

    if not isinstance(raw.get("pcDevice"), bool) or not isinstance(raw.get("mobileDevice"), bool):
        try:
            res_target, pm_target_obj = _fetch_target_object(api_key, secret_key, cid, adgroup_id, "PC_MOBILE_TARGET")
            if res_target.status_code != 200:
                pm_target_obj = None
        except Exception:
            pm_target_obj = None

    pc, mobile = _extract_pc_mobile_flags(detail_obj if isinstance(detail_obj, dict) else raw, pm_target_obj)
    if isinstance(pc, bool):
        raw["pcDevice"] = pc
    if isinstance(mobile, bool):
        raw["mobileDevice"] = mobile

    target_summary = raw.get("targetSummary") if isinstance(raw.get("targetSummary"), dict) else {}
    media_label = _pc_mobile_label(raw.get("pcDevice"), raw.get("mobileDevice"))
    if media_label != "UNKNOWN":
        target_summary["pcMobile"] = media_label
        raw["targetSummary"] = target_summary
        row["mediaType"] = media_label

    row["pcDevice"] = raw.get("pcDevice")
    row["mobileDevice"] = raw.get("mobileDevice")
    row["raw"] = raw
    return row


def _fetch_adgroups(api_key: str, secret_key: str, cid: str, campaign_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": campaign_id})
    if res.status_code != 200:
        return res, []
    rows = [_normalize_adgroup_item(x) for x in (res.json() or [])]
    if rows:
        max_workers = min(8, max(1, len(rows)))
        enriched_rows: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {ex.submit(_enrich_adgroup_media_row, api_key, secret_key, cid, row): idx for idx, row in enumerate(rows)}
            ordered: Dict[int, Dict[str, Any]] = {}
            for fut in as_completed(future_map):
                idx = future_map[fut]
                try:
                    ordered[idx] = fut.result()
                except Exception:
                    ordered[idx] = rows[idx]
            enriched_rows = [ordered[i] for i in range(len(rows))]
        rows = enriched_rows
    rows.sort(key=lambda x: x["name"])
    return res, rows

def _fetch_adgroup_detail(api_key: str, secret_key: str, cid: str, adgroup_id: str):
    res = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}")
    if res.status_code == 200 and isinstance(res.json(), dict):
        return res, res.json()
    return res, None


def _fetch_first_biz_channel_id(api_key: str, secret_key: str, cid: str) -> str:
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/channels")
    if res.status_code != 200:
        return ""
    for item in (res.json() or []):
        norm = _normalize_channel_item(item if isinstance(item, dict) else {})
        cid_val = str(norm.get("id") or "").strip()
        if cid_val:
            return cid_val
    return ""

def _is_shopping_adgroup(adgroup_obj: Dict[str, Any] | None) -> bool:
    adg_type = str((adgroup_obj or {}).get("adgroupType") or "").upper()
    return adg_type in {"SHOPPING", "CATALOG", "SHOPPING_BRAND"}


def _fetch_keywords(api_key: str, secret_key: str, cid: str, adgroup_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adgroup_id})
    if res.status_code != 200:
        return res, []
    return res, res.json() or []


def _fetch_ads(api_key: str, secret_key: str, cid: str, adgroup_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": adgroup_id})
    if res.status_code != 200:
        return res, []
    return res, res.json() or []


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


def _copy_target_payload_exact(api_key: str, secret_key: str, cid: str, source_owner_id: str, target_owner_id: str, target_type: str):
    res_src, src_target_obj = _fetch_target_object(api_key, secret_key, cid, source_owner_id, target_type)
    if res_src.status_code != 200:
        return False, f"{target_type} 원본 조회 실패: {res_src.text}"
    if not src_target_obj:
        return False, f"{target_type} 원본 설정 없음"
    src_target = copy.deepcopy(src_target_obj.get("target"))
    if src_target is None:
        return False, f"{target_type} 원본 target 비어 있음"

    res_dst, dst_target_obj = _fetch_target_object(api_key, secret_key, cid, target_owner_id, target_type)
    if res_dst.status_code == 200 and dst_target_obj and dst_target_obj.get("nccTargetId"):
        payload = {
            "nccTargetId": dst_target_obj.get("nccTargetId"),
            "ownerId": target_owner_id,
            "targetTp": target_type,
            "target": src_target,
            "delFlag": False,
        }
        res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/targets/{dst_target_obj.get('nccTargetId')}", json_body=payload)
        if res_put.status_code in [200, 201]:
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

    return False, f"{target_type} 대상 타겟 조회 실패: {res_dst.text if res_dst is not None else '알 수 없는 오류'}"


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


def _update_adgroup_search_options(api_key: str, secret_key: str, cid: str, adgroup_id: str, use_keyword_plus: Optional[bool] = None, keyword_plus_weight: Optional[int] = None, use_close_variant: Optional[bool] = None):
    if use_keyword_plus is None and keyword_plus_weight is None and use_close_variant is None:
        return True, "변경사항 없음"
    res_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}")
    if res_get.status_code != 200:
        return False, f"광고그룹 조회 실패: {res_get.text}"
    obj = res_get.json() or {}
    fields: List[str] = []
    ignored_msgs: List[str] = []

    if use_keyword_plus is not None:
        obj["useKeywordPlus"] = bool(use_keyword_plus)
        fields.append("useKeywordPlus")
        if keyword_plus_weight is None:
            keyword_plus_weight = int(obj.get("keywordPlusWeight") or 100)
        try:
            keyword_plus_weight = max(1, min(999999, int(keyword_plus_weight)))
        except Exception:
            keyword_plus_weight = 100
        obj["keywordPlusWeight"] = int(keyword_plus_weight)
        fields.append("keywordPlusWeight")
    elif keyword_plus_weight is not None:
        try:
            keyword_plus_weight = max(1, min(999999, int(keyword_plus_weight)))
        except Exception:
            keyword_plus_weight = 100
        obj["keywordPlusWeight"] = int(keyword_plus_weight)
        fields.append("keywordPlusWeight")

    if use_close_variant is not None:
        ignored_msgs.append("일치검색은 API 수정 미지원으로 적용 제외")

    fields = list(dict.fromkeys(fields))
    if not fields:
        return True, " / ".join(ignored_msgs) if ignored_msgs else "변경사항 없음"

    res_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}", params={"fields": ",".join(fields)}, json_body=obj)
    if res_put.status_code in [200, 201]:
        msg = "광고그룹 검색옵션 업데이트 완료"
        if ignored_msgs:
            msg += " / " + " / ".join(ignored_msgs)
        return True, msg
    return False, res_put.text


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
    ext_id = str(ext_item.get("adExtensionId") or ext_item.get("id") or "").strip()
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
    ext_id = str(ext_item.get("adExtensionId") or ext_item.get("id") or "").strip()
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
    ext_id = str(ext_item.get("adExtensionId") or ext_item.get("id") or "").strip()
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

        def _post_ad(ad):
            src_ad = ad if isinstance(ad, dict) else {}
            src_ad_id = _extract_ad_id(src_ad)
            item = _build_copy_ad_payload(api_key, secret_key, cid, src_ad, str(new_adg_id))
            ad_type = _normalize_ad_type(item.get("type"))
            item.setdefault("userLock", False)
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
    for k in ["useStoreUrl", "nccProductGroupId", "contentsNetworkBidAmt", "keywordPlusFlag", "contractId"]:
        if k in src:
            res[k] = src[k]
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


@app.route("/get_campaigns", methods=["POST"])
def get_campaigns():
    d = request.json or {}
    res, rows = _fetch_campaigns(d.get("api_key"), d.get("secret_key"), d.get("customer_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "캠페인 조회 실패", "details": res.text}), 400


@app.route("/get_adgroups", methods=["POST"])
def get_adgroups():
    d = request.json or {}
    campaign_id = str(d.get("campaign_id") or "").strip()
    if not campaign_id:
        return jsonify([])
    res, rows = _fetch_adgroups(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), campaign_id)
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "광고그룹 조회 실패", "details": res.text}), 400


@app.route("/get_biz_channels", methods=["POST"])
def get_biz_channels():
    d = request.json or {}
    res = _do_req("GET", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/channels")
    if res.status_code == 200:
        normalized = [_normalize_channel_item(item) for item in (res.json() or [])]
        return jsonify(normalized)
    return jsonify({"error": "비즈채널 조회 실패", "details": res.text}), 400


@app.route("/get_keywords", methods=["POST"])
def get_keywords():
    d = request.json or {}
    res, rows = _fetch_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "키워드 조회 실패", "details": res.text}), 400


@app.route("/get_ads", methods=["POST"])
def get_ads():
    d = request.json or {}
    res, rows = _fetch_ads(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "소재 조회 실패", "details": res.text}), 400


@app.route("/get_ad_extensions", methods=["POST"])
def get_ad_extensions():
    d = request.json or {}
    res, rows = _fetch_extensions(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("owner_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "확장소재 조회 실패", "details": res.text}), 400


@app.route("/get_restricted_keywords", methods=["POST"])
def get_restricted_keywords():
    d = request.json or {}
    res, rows = _fetch_restricted_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_id"))
    if res.status_code == 200:
        return jsonify(rows)
    return jsonify({"error": "제외키워드 조회 실패", "details": res.text}), 400


@app.route("/create_campaign", methods=["POST"])
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
        return jsonify({"ok": True, "item": res.json(), "message": "캠페인 생성 완료"})
    return jsonify({"error": "캠페인 생성 실패", "details": res.text}), 400


@app.route("/create_adgroup_simple", methods=["POST"])
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
                ukp = None if use_keyword_plus is None else bool(use_keyword_plus)
                ucv = None if use_close_variant is None else bool(use_close_variant)
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


@app.route("/create_keywords_simple", methods=["POST"])

def create_keywords_simple():
    d = request.json or {}
    adgroup_id = str(d.get("adgroup_id") or "").strip()
    keywords = [x.strip() for x in str(d.get("keywords") or "").replace(",", "\n").splitlines() if x.strip()]
    if not adgroup_id or not keywords:
        return jsonify({"error": "광고그룹과 키워드는 필수입니다."}), 400
    rows = [{
        "nccAdgroupId": adgroup_id,
        "keyword": kw,
        "useGroupBidAmt": bool(d.get("use_group_bid_amt", True)),
        "bidAmt": int(d.get("bid_amt") or 70),
        "userLock": bool(d.get("user_lock", False)),
    } for kw in keywords]
    success, fail, results = _bulk_create_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), rows)
    return jsonify({"ok": True, "total": len(rows), "success": success, "fail": fail, "results": results})


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


@app.route("/bulk_upload_text_ads", methods=["POST"])
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


@app.route("/create_text_ad_simple", methods=["POST"])
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


@app.route("/create_ad_advanced", methods=["POST"])
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



@app.route("/create_extension_simple", methods=["POST"])
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


@app.route("/bulk_upload_headlines", methods=["POST"])
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


@app.route("/create_shopping_ad_simple", methods=["POST"])
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


@app.route("/create_extension_raw", methods=["POST"])
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


@app.route("/create_restricted_keywords_simple", methods=["POST"])
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



@app.route("/copy_entities_to_adgroups", methods=["POST"])
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
        msg += "\n" + "\n".join(all_errors[:10])
    return jsonify({"ok": True, "message": msg})


@app.route("/copy_campaigns", methods=["POST"])
def copy_campaigns():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    src_ids, suffix = d.get("source_ids", []), d.get("suffix", "_복사본")
    results, all_errors = {"success": 0, "fail": 0}, []
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/campaigns/{src_id}")
        if r_get.status_code != 200:
            results["fail"] += 1
            all_errors.append(f"[원본 캠페인 {src_id}] 조회 실패: {r_get.text}")
            continue
        src = r_get.json()
        new_camp = {
            "customerId": int(cid),
            "name": src.get("name", "") + suffix,
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

        r_adgs, src_adgroups = _fetch_adgroups(api_key, secret_key, cid, str(src_id))
        if r_adgs.status_code != 200:
            all_errors.append(f"[{new_camp['name']}] 원본 광고그룹 조회 실패: {r_adgs.text}")
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
    msg = f"캠페인 복사 완료!\n(성공: {results['success']}개, 실패: {results['fail']}개)"
    if all_errors:
        msg += "\n" + "\n".join(all_errors[:10])
    return jsonify({"ok": True, "message": msg})


@app.route("/copy_adgroups_to_target", methods=["POST"])
def copy_adgroups_to_target():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    src_ids = _unique_keep_order(d.get("source_ids") or [])
    target_camp_id = d.get("target_campaign_id")
    suffix = d.get("suffix", "_복사본")
    biz_channel_id = d.get("biz_channel_id")
    include_keywords = _boolish(d.get("include_keywords"), True)
    include_ads = _boolish(d.get("include_ads"), True)
    include_extensions = _boolish(d.get("include_extensions"), True)
    include_negatives = _boolish(d.get("include_negatives"), True)
    copy_media = _boolish(d.get("copy_media"), True)
    copy_as_off = _boolish(d.get("copy_as_off"), False)
    results, all_errors = {"success": 0, "fail": 0}, []
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{src_id}")
        if r_get.status_code != 200:
            results["fail"] += 1
            all_errors.append(f"[원본 {src_id}] 조회 실패: {r_get.text}")
            continue
        new_adg = _extract_adgroup(r_get.json(), target_camp_id, cid, biz_channel_id)
        new_adg["name"] = str(new_adg.get("name") or "") + suffix
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

            if copy_as_off and new_adg_id:
                ok_off, off_msg = _set_user_lock_for_entity(api_key, secret_key, cid, "adgroup", new_adg_id, False)
                if not ok_off and off_msg:
                    all_errors.append(f"[{new_adg['name']}] OFF 설정 실패: {off_msg}")
        else:
            results["fail"] += 1
            all_errors.append(f"[{new_adg['name']}] 생성 실패: {r_post.text}")
    return jsonify({"ok": True, "message": f"복사 완료! (성공: {results['success']}, 실패: {results['fail']})\n" + "\n".join(all_errors[:10])})


@app.route("/update_media", methods=["POST"])
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


@app.route("/update_adgroup_options", methods=["POST"])
def update_adgroup_options():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_ids = [str(x).strip() for x in (d.get("entity_ids") or []) if str(x).strip()]
    media_type = d.get("media_type")
    media_detail = _normalize_media_detail(d.get("media_detail"))
    use_keyword_plus = d.get("use_keyword_plus")
    use_close_variant = d.get("use_close_variant")
    keyword_plus_weight = d.get("keyword_plus_weight")
    if not entity_ids:
        return jsonify({"error": "선택된 광고그룹이 없습니다."}), 400

    results = []
    success = fail = skipped = 0
    for adg_id in entity_ids:
        detail_res, adgroup_obj = _fetch_adgroup_detail(api_key, secret_key, cid, adg_id)
        if detail_res.status_code != 200 or not adgroup_obj:
            fail += 1
            results.append({"nccAdgroupId": adg_id, "ok": False, "detail": f"광고그룹 조회 실패: {detail_res.text}"})
            continue

        row_msgs: List[str] = []
        row_ok = True

        if str(media_type or "").strip() != "" or d.get("media_detail") is not None:
            ok_media_pm, detail_media_pm = _update_pc_mobile_target(api_key, secret_key, cid, adg_id, media_type or "ALL")
            row_msgs.append(f"PC/모바일 매체: {detail_media_pm}")
            row_ok = row_ok and ok_media_pm
            ok_media_network, detail_media_network = _update_media_target(api_key, secret_key, cid, adg_id, media_detail)
            row_msgs.append(f"세부 매체: {detail_media_network}")
            row_ok = row_ok and ok_media_network

        is_web_site = str(adgroup_obj.get("adgroupType") or "").upper() == "WEB_SITE"
        if use_keyword_plus is not None or use_close_variant is not None or str(keyword_plus_weight or "").strip() != "":
            if not is_web_site:
                skipped += 1
                row_msgs.append("파워링크 그룹이 아니어서 확장검색/일치검색은 건너뜀")
            else:
                kwp = None
                if str(keyword_plus_weight or "").strip() != "":
                    try:
                        kwp = int(keyword_plus_weight)
                    except Exception:
                        kwp = None
                ok_opts, detail_opts = _update_adgroup_search_options(api_key, secret_key, cid, adg_id, use_keyword_plus=None if use_keyword_plus is None else bool(use_keyword_plus), keyword_plus_weight=kwp, use_close_variant=None if use_close_variant is None else bool(use_close_variant))
                row_msgs.append(f"검색옵션: {detail_opts}")
                row_ok = row_ok and ok_opts

        if row_ok:
            success += 1
        else:
            fail += 1
        results.append({"nccAdgroupId": adg_id, "ok": row_ok, "detail": " | ".join(row_msgs) if row_msgs else "변경 완료"})

    status_code = 200 if success > 0 else 400
    return jsonify({
        "ok": success > 0,
        "message": f"총 {len(entity_ids)}개 광고그룹 설정 변경 완료 · 성공 {success}개 / 실패 {fail}개 / 건너뜀 {skipped}개",
        "success": success,
        "fail": fail,
        "skipped": skipped,
        "results": results,
    }), status_code


@app.route("/update_budget", methods=["POST"])
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
    raw: List[int] = []
    if not isinstance(values, list):
        return raw
    uniq: List[int] = []
    seen = set()
    for v in values:
        try:
            n = int(v)
        except Exception:
            continue
        if 0 <= n <= 23 and n not in seen:
            seen.add(n)
            uniq.append(n)
    uniq.sort()

    if not uniq:
        return []

    is_contiguous = len(uniq) >= 2 and all(uniq[i] == uniq[i - 1] + 1 for i in range(1, len(uniq)))
    if is_contiguous:
        return uniq[:-1]

    return uniq


def _build_schedule_codes(days: List[int], hours: List[int]) -> List[str]:
    codes: List[str] = []
    for d_num in days:
        day_code = DAY_NUM_TO_CODE.get(int(d_num))
        if not day_code:
            continue
        for h in hours:
            start_h = int(h)
            end_h = start_h + 1
            if 0 <= start_h <= 23 and 1 <= end_h <= 24:
                codes.append(f"SD{day_code}{start_h:02d}{end_h:02d}")
    return codes


@app.route("/update_schedule", methods=["POST"])
def update_schedule():
    d = request.get_json(silent=True) or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")

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

    try:
        bid_weight = int(d.get("bidWeight", 100))
    except Exception:
        bid_weight = 100

    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not adgroup_ids:
        return jsonify({"error": "광고그룹이 선택되지 않았습니다."}), 400
    if not days:
        return jsonify({"error": "요일을 1개 이상 선택해주세요."}), 400
    if not raw_hours:
        return jsonify({"error": "시간을 1개 이상 선택해주세요."}), 400
    if not hours:
        return jsonify({"error": "연속 시간 선택 시 마지막 시간은 종료시각으로 처리됩니다. 예: 8~20 선택 → 실제 적용 8~19"}), 400

    codes = _build_schedule_codes(days, hours)

    results = []
    success = 0
    fail = 0

    for owner_id in adgroup_ids:
        owner_id = str(owner_id).strip()
        uri = f"/ncc/criterion/{owner_id}/SD"
        target_body = [{"customerId": int(cid), "ownerId": owner_id, "dictionaryCode": c, "type": "SD"} for c in codes]

        put_res = _do_req("PUT", api_key, secret_key, cid, uri, json_body=target_body)
        if put_res.status_code != 200:
            fail += 1
            results.append({
                "ownerId": owner_id,
                "ok": False,
                "step": "criterion_put",
                "details": put_res.text,
                "codes": codes,
            })
            continue

        if bid_weight != 100 and codes:
            bid_fail = False
            bid_error_text = ""
            for i in range(0, len(codes), 50):
                chunk = codes[i:i + 50]
                bw_res = _do_req(
                    "PUT",
                    api_key,
                    secret_key,
                    cid,
                    f"/ncc/criterion/{owner_id}/bidWeight",
                    params={"codes": ",".join(chunk), "bidWeight": bid_weight},
                )
                if bw_res.status_code != 200:
                    bid_fail = True
                    bid_error_text = bw_res.text
                    break
            if bid_fail:
                fail += 1
                results.append({
                    "ownerId": owner_id,
                    "ok": False,
                    "step": "bid_weight_put",
                    "details": bid_error_text,
                    "codes": codes,
                })
                continue

        success += 1
        results.append({"ownerId": owner_id, "ok": True, "codes": codes})

    status_code = 200 if success > 0 else 400
    return jsonify({
        "ok": success > 0,
        "message": f"총 {len(adgroup_ids)}개 스케줄 업데이트 성공: {success}개 / 실패: {fail}개",
        "success": success,
        "fail": fail,
        "raw_hours": raw_hours,
        "applied_hours": hours,
        "results": results[:20],
    }), status_code


@app.route("/update_schedule_campaign_bulk", methods=["POST"])
def update_schedule_campaign_bulk():
    d = request.get_json(silent=True) or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")

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

    try:
        bid_weight = int(d.get("bidWeight", 100))
    except Exception:
        bid_weight = 100

    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if not campaign_ids:
        return jsonify({"error": "캠페인이 선택되지 않았습니다."}), 400
    if not days:
        return jsonify({"error": "요일을 1개 이상 선택해주세요."}), 400
    if not raw_hours:
        return jsonify({"error": "시간을 1개 이상 선택해주세요."}), 400
    if not hours:
        return jsonify({"error": "연속 시간 선택 시 마지막 시간은 종료시각으로 처리됩니다. 예: 8~20 선택 → 실제 적용 8~19"}), 400

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

    codes = _build_schedule_codes(days, hours)

    results = []
    success = 0
    fail = 0
    for owner_id in adgroup_ids:
        owner_id = str(owner_id).strip()
        uri = f"/ncc/criterion/{owner_id}/SD"
        target_body = [{"customerId": int(cid), "ownerId": owner_id, "dictionaryCode": c, "type": "SD"} for c in codes]

        put_res = _do_req("PUT", api_key, secret_key, cid, uri, json_body=target_body)
        if put_res.status_code != 200:
            fail += 1
            results.append({
                "ownerId": owner_id,
                "ok": False,
                "step": "criterion_put",
                "details": put_res.text,
                "codes": codes,
            })
            continue

        if bid_weight != 100 and codes:
            bid_fail = False
            bid_error_text = ""
            for i in range(0, len(codes), 50):
                chunk = codes[i:i + 50]
                bw_res = _do_req(
                    "PUT",
                    api_key,
                    secret_key,
                    cid,
                    f"/ncc/criterion/{owner_id}/bidWeight",
                    params={"codes": ",".join(chunk), "bidWeight": bid_weight},
                )
                if bw_res.status_code != 200:
                    bid_fail = True
                    bid_error_text = bw_res.text
                    break
            if bid_fail:
                fail += 1
                results.append({
                    "ownerId": owner_id,
                    "ok": False,
                    "step": "bid_weight_put",
                    "details": bid_error_text,
                    "codes": codes,
                })
                continue

        success += 1
        results.append({"ownerId": owner_id, "ok": True, "codes": codes})

    status_code = 200 if success > 0 else 400
    return jsonify({
        "ok": success > 0,
        "message": f"하위 광고그룹 총 {len(adgroup_ids)}개 스케줄 변경 성공: {success}개 / 실패: {fail}개",
        "success": success,
        "fail": fail,
        "raw_hours": raw_hours,
        "applied_hours": hours,
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


def _estimate_keyword_bids_by_avg_position(api_key: str, secret_key: str, cid: str, keyword_ids: List[str], device: str, position: int, max_bid: Optional[int] = None):
    estimated: Dict[str, int] = {}
    warnings: List[str] = []
    device = str(device or "PC").upper()
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
            warnings.append(f"평균순위 추정 실패({device}, {position}위): {res.text}")
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
            warnings.append(f"평균순위 추정 응답 형식이 예상과 다릅니다: {payload}")
            continue
        for item in items:
            try:
                key = str(item.get("key") or item.get("nccKeywordId") or "").strip()
                bid = _normalize_bid_amt(item.get("bid"), max_bid=max_bid)
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


@app.route("/update_keyword_bids", methods=["POST"])
def update_keyword_bids():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type, entity_ids = d.get("entity_type"), d.get("entity_ids", [])
    bid_amt = int(d.get("bid_amt", 70))
    if not entity_ids:
        return jsonify({"error": "대상이 없습니다."}), 400
    adgroup_ids = []
    if entity_type == "campaign":
        for camp_id in entity_ids:
            r_adgs = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": camp_id})
            if r_adgs.status_code == 200:
                adgroup_ids.extend([adg.get("nccAdgroupId") for adg in r_adgs.json() or []])
    else:
        adgroup_ids = entity_ids
    use_group_bid = bid_amt == 0
    target_bid = bid_amt if bid_amt >= 70 else 70
    success_cnt, fail_cnt = 0, 0
    err_details = []
    for adg_id in adgroup_ids:
        r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id})
        if r_kw.status_code == 200:
            kws = r_kw.json() or []
            if not kws:
                continue
            update_payload = []
            for kw in kws:
                item = copy.deepcopy(kw)
                item["useGroupBidAmt"] = use_group_bid
                item["bidAmt"] = target_bid
                for k in ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']:
                    item.pop(k, None)
                update_payload.append(item)
            for i in range(0, len(update_payload), 100):
                batch = update_payload[i:i + 100]
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
                                err_details.append(f"[{item.get('keyword', '알수없음')}] 실패: {r_single.text}")
    msg = f"키워드 입찰가 변경 완료!\n(성공: {success_cnt}개, 실패: {fail_cnt}개)"
    if err_details:
        msg += "\n\n[상세 에러 내역]\n" + "\n".join(err_details)
    return jsonify({"ok": True, "message": msg})




@app.route("/update_keyword_bids_avg_position", methods=["POST"])
def update_keyword_bids_avg_position():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = str(d.get("entity_type") or "adgroup").strip()
    entity_ids = d.get("entity_ids", []) or []
    device = str(d.get("device") or "PC").upper()
    position = int(d.get("position") or 1)
    preview_only = bool(d.get("preview_only"))
    include_paused = bool(d.get("include_paused"))
    include_pending = bool(d.get("include_pending"))
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

    adgroup_ids, skipped_non_web, warnings = _resolve_web_site_adgroup_ids(api_key, secret_key, cid, entity_type, entity_ids)
    if not adgroup_ids:
        msg = "파워링크 광고그룹이 없어 평균순위 입찰가를 적용할 수 없습니다."
        if warnings:
            msg += "\n" + "\n".join(warnings[:5])
        return jsonify({"error": msg}), 400

    keyword_ids: List[str] = []
    keyword_meta: Dict[str, Dict[str, Any]] = {}
    keyword_count = 0
    fetch_fail = 0
    skipped_paused = 0
    skipped_pending = 0
    for adg_id in adgroup_ids:
        r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id})
        if r_kw.status_code != 200:
            fetch_fail += 1
            if len(warnings) < 5:
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
                    skipped_paused += 1
                else:
                    skipped_pending += 1
                continue
            keyword_ids.append(kid)
            keyword_meta[kid] = {
                "keyword": str(kw.get("keyword") or ""),
                "current_bid": kw.get("bidAmt"),
                "use_group_bid": bool(kw.get("useGroupBidAmt")),
                "adgroup_id": adg_id,
            }

    keyword_ids = _unique_keep_order(keyword_ids)
    if not keyword_ids:
        return jsonify({"error": "평균순위 추정에 사용할 활성 키워드가 없습니다."}), 400

    estimated_bid_map, estimate_warnings = _estimate_keyword_bids_by_avg_position(api_key, secret_key, cid, keyword_ids, device, position, max_bid=max_bid)
    warnings.extend(estimate_warnings)
    if not estimated_bid_map:
        msg = f"{device} {position}위 평균순위 추정값을 받지 못했습니다."
        if warnings:
            msg += "\n" + "\n".join(warnings[:5])
        return jsonify({"error": msg}), 400

    preview_rows = []
    changed_bids = []
    unchanged_cnt = 0
    for kid in keyword_ids:
        if kid not in estimated_bid_map:
            continue
        meta = keyword_meta.get(kid, {})
        current_bid = _normalize_bid_amt(meta.get("current_bid"))
        new_bid = estimated_bid_map[kid]
        will_change = (current_bid != new_bid) or bool(meta.get("use_group_bid"))
        if will_change:
            changed_bids.append(new_bid)
        else:
            unchanged_cnt += 1
        if len(preview_rows) < 30:
            preview_rows.append({
                "keyword": meta.get("keyword") or kid,
                "keyword_id": kid,
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

    stats = _calc_stats(list(estimated_bid_map.values()))
    if preview_only:
        lines = [
            f"{device} 평균순위 {position}위 기준 변경사항 확인 완료",
            f"추정 성공 키워드: {len(estimated_bid_map)}개 / 적용 대상 키워드: {len(keyword_ids)}개",
            f"변경 예정: {len(changed_bids)}개 / 동일해서 유지: {unchanged_cnt}개",
        ]
        if stats["min"] is not None:
            lines.append(f"예상 입찰가 범위: 최소 {stats['min']:,}원 · 중앙 {stats['median']:,}원 · 최대 {stats['max']:,}원")
        if max_bid is not None:
            lines.append(f"최대 입찰가 상한 적용: {max_bid:,}원")
        if skipped_paused:
            lines.append(f"중지 상태 제외: {skipped_paused}개")
        if skipped_pending:
            lines.append(f"검수보류/비승인 제외: {skipped_pending}개")
        if skipped_non_web:
            lines.append(f"쇼핑/기타 광고그룹 제외: {skipped_non_web}개")
        for msg in warnings[:5]:
            lines.append(msg)
        return jsonify({
            "ok": True,
            "preview": True,
            "message": "\n".join(lines),
            "stats": stats,
            "rows": preview_rows,
            "estimated": len(estimated_bid_map),
            "changed": len(changed_bids),
            "unchanged": unchanged_cnt,
            "skipped_paused": skipped_paused,
            "skipped_pending": skipped_pending,
        })

    success_cnt, fail_cnt, skipped_cnt, err_details = _apply_keyword_bid_map(api_key, secret_key, cid, adgroup_ids, estimated_bid_map, keyword_meta=keyword_meta)
    lines = [
        f"{device} 평균순위 {position}위 기준 입찰가 적용 완료!",
        f"파워링크 광고그룹: {len(adgroup_ids)}개",
        f"추정 성공 키워드: {len(estimated_bid_map)}개 / 전체 조회 키워드: {keyword_count}개",
        f"입찰가 변경 성공: {success_cnt}개 / 실패: {fail_cnt}개 / 동일해서 유지: {unchanged_cnt}개",
    ]
    if stats["min"] is not None:
        lines.append(f"예상 입찰가 범위: 최소 {stats['min']:,}원 · 중앙 {stats['median']:,}원 · 최대 {stats['max']:,}원")
    if max_bid is not None:
        lines.append(f"최대 입찰가 상한 적용: {max_bid:,}원")
    if skipped_non_web:
        lines.append(f"쇼핑/기타 광고그룹 건너뜀: {skipped_non_web}개")
    if fetch_fail:
        lines.append(f"키워드 조회 실패 광고그룹: {fetch_fail}개")
    if skipped_paused:
        lines.append(f"중지 상태 제외: {skipped_paused}개")
    if skipped_pending:
        lines.append(f"검수보류/비승인 제외: {skipped_pending}개")
    if skipped_cnt:
        lines.append(f"추정값 없음/변경 생략 키워드: {skipped_cnt}개")
    for msg in (warnings[:5] + err_details[:5]):
        lines.append(msg)
    return jsonify({
        "ok": True,
        "message": "\n".join(lines),
        "estimated": len(estimated_bid_map),
        "success": success_cnt,
        "fail": fail_cnt,
        "skipped": skipped_cnt,
        "skipped_non_web": skipped_non_web,
        "unchanged": unchanged_cnt,
        "stats": stats,
    })

def _delete_payload_rows(api_key: str, secret_key: str, cid: str, entity_type: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        name = f"{idx}행"
        if entity_type in {"campaign", "adgroup", "keyword", "ad", "ad_extension"}:
            key_map = {
                "campaign": ["nccCampaignId", "캠페인ID", "campaign_id"],
                "adgroup": ["nccAdgroupId", "광고그룹ID", "adgroup_id"],
                "keyword": ["nccKeywordId", "키워드ID", "keyword_id"],
                "ad": ["nccAdId", "소재ID", "ad_id"],
                "ad_extension": ["adExtensionId", "확장소재ID", "ad_extension_id"],
            }
            entity_id = ""
            for k in key_map[entity_type]:
                entity_id = str(row.get(k) or "").strip()
                if entity_id:
                    break
            res = _delete_entity_by_id(api_key, secret_key, cid, entity_type, entity_id)
            name = entity_id or name
            if res.status_code in [200, 201, 204]:
                success += 1
                results.append(_result_item(idx, True, name, "삭제 완료"))
            else:
                fail += 1
                results.append(_result_item(idx, False, name, getattr(res, 'text', '삭제 실패')))
        elif entity_type == "restricted_keyword":
            adgroup_id = str(row.get("nccAdgroupId") or row.get("광고그룹ID") or row.get("adgroup_id") or "").strip()
            keyword = str(row.get("keyword") or row.get("제외키워드") or "").strip()
            name = keyword or name
            if not adgroup_id or not keyword:
                fail += 1
                results.append(_result_item(idx, False, name, "광고그룹ID / 제외키워드는 필수입니다."))
                continue
            res = _do_req("DELETE", api_key, secret_key, cid, "/ncc/restricted-keywords", params={"nccAdgroupId": adgroup_id, "keyword": keyword})
            if res.status_code in [200, 201, 204]:
                success += 1
                results.append(_result_item(idx, True, name, "삭제 완료"))
            else:
                fail += 1
                results.append(_result_item(idx, False, name, getattr(res, 'text', '삭제 실패')))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, "지원하지 않는 삭제 유형"))
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
        for campaign_id in parent_ids:
            campaign_id = str(campaign_id or "").strip()
            if not campaign_id:
                continue
            res, rows = _fetch_adgroups(api_key, secret_key, cid, campaign_id)
            if res.status_code != 200:
                errors.append(f"[캠페인 {campaign_id}] 광고그룹 조회 실패: {res.text}")
                continue
            for row in rows:
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
    # 쇼핑 추가홍보문구는 계정/생성 경로에 따라 캠페인 owner, 광고그룹 owner,
    # 또는 쇼핑 소재(ownerId=nccAdId) 기준으로 내려올 수 있어 가능성을 모두 조회한다.
    include_campaign_owner = bool(campaign_ids)
    include_adgroup_owner = True
    include_ad_owner = shopping_promo_only or normalized_type == "ALL"

    def _push_ext_items(items: List[Dict[str, Any]], owner_label: str):
        for item in (items or []):
            if not isinstance(item, dict):
                continue
            ext_id = str(item.get("adExtensionId") or item.get("id") or "").strip()
            if not ext_id or ext_id in seen_ext_ids:
                continue
            item_type = _normalize_bulk_extension_delete_type(item.get("type"))
            if shopping_promo_only:
                if item_type != "PROMOTION":
                    continue
            else:
                if normalized_type != "ALL" and item_type != normalized_type:
                    continue
            seen_ext_ids.add(ext_id)
            rows.append({"adExtensionId": ext_id, "_owner": owner_label, "_type": item_type})

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
                if not (adgroup_is_shopping or _looks_like_shopping_ad(ad_item)):
                    continue
                res_ad_ext, ad_ext_items = _fetch_extensions(api_key, secret_key, cid, ad_id)
                if res_ad_ext.status_code == 200:
                    _push_ext_items(ad_ext_items, f"ad:{ad_id}")
                else:
                    errors.append(f"[소재 {ad_id}] 확장소재 조회 실패: {res_ad_ext.text}")

    return rows, errors


@app.route("/bulk_delete_by_parent", methods=["POST"])
def bulk_delete_by_parent():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    parent_type = str(d.get("parent_type") or "campaign").strip()
    parent_ids = [str(x).strip() for x in (d.get("parent_ids") or []) if str(x).strip()]
    target_entity = str(d.get("target_entity") or "").strip()
    ext_type = str(d.get("ext_type") or "ALL").strip() or "ALL"

    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400
    if parent_type not in {"campaign", "adgroup"}:
        return jsonify({"error": "지원하지 않는 부모 범위입니다."}), 400
    if target_entity not in {"keyword", "ad", "extension"}:
        return jsonify({"error": "지원하지 않는 삭제 대상입니다."}), 400
    if not parent_ids:
        return jsonify({"error": "선택된 대상이 없습니다."}), 400

    adgroup_ids, collect_errors = _collect_target_adgroup_ids(api_key, secret_key, cid, parent_type, parent_ids)
    if not adgroup_ids and collect_errors:
        return jsonify({"error": "하위 광고그룹 조회에 실패했습니다.", "details": collect_errors[:10]}), 400

    rows: List[Dict[str, Any]] = []
    entity_type = {"keyword": "keyword", "ad": "ad", "extension": "ad_extension"}[target_entity]
    seen = set()

    if target_entity == "keyword":
        for adgroup_id in adgroup_ids:
            res, items = _fetch_keywords(api_key, secret_key, cid, adgroup_id)
            if res.status_code != 200:
                collect_errors.append(f"[광고그룹 {adgroup_id}] 키워드 조회 실패: {res.text}")
                continue
            for item in (items or []):
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
        campaign_ids_for_ext = parent_ids if parent_type == "campaign" else []
        rows, ext_errors = _collect_extension_delete_rows(api_key, secret_key, cid, adgroup_ids, ext_type, campaign_ids=campaign_ids_for_ext)
        collect_errors.extend(ext_errors)

    if not rows:
        scope_label = "캠페인" if parent_type == "campaign" else "광고그룹"
        target_label = {
            "keyword": "키워드",
            "ad": "소재",
            "extension": "확장소재",
        }[target_entity]
        if str(ext_type or "").strip().upper() == "SHOPPING_PROMO_TEXT":
            target_label = "쇼핑 추가홍보문구"
        msg = f"선택한 {scope_label} 범위에서 삭제할 {target_label}가 없습니다."
        if str(ext_type or "").strip().upper() == "SHOPPING_PROMO_TEXT" and parent_type == "campaign":
            msg += "\n(캠페인/광고그룹/쇼핑소재 owner 기준으로 모두 조회했지만 대상이 발견되지 않았습니다.)"
        if collect_errors:
            msg += "\n" + "\n".join(collect_errors[:10])
        return jsonify({"ok": True, "total": 0, "success": 0, "fail": 0, "results": [], "message": msg})

    success, fail, results = _delete_payload_rows(api_key, secret_key, cid, entity_type, rows)
    msg_target = {
        "keyword": "키워드",
        "ad": "소재",
        "extension": "확장소재",
    }[target_entity]
    if str(ext_type or "").strip().upper() == "SHOPPING_PROMO_TEXT":
        msg_target = "쇼핑 추가홍보문구"
    msg = f"{msg_target} 일괄 삭제 완료 (대상 {len(rows)}건 / 성공 {success} / 실패 {fail})"
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
    })


@app.route("/bulk_register", methods=["POST"])
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


@app.route("/bulk_delete", methods=["POST"])
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


@app.route("/set_campaign_state", methods=["POST"])
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
    return jsonify({"ok": True, "message": msg})


@app.route("/delete_selected", methods=["POST"])
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


if __name__ == "__main__":
    os.makedirs(SAMPLES_DIR, exist_ok=True)
    app.run(debug=True, port=5000)
