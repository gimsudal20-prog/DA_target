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


def _looks_like_biz_channel_id(value: Any) -> bool:
    return bool(re.match(r"^bsn-", str(value or "").strip(), re.I))


def _normalize_campaign_tp(value: Any) -> str:
    s = str(value or "").strip().upper()
    if s in {"파워링크", "POWERLINK", "WEB", "WEB_SITE"}:
        return "WEB_SITE"
    if s in {"쇼핑검색", "쇼검", "SHOPPING"}:
        return "SHOPPING"
    return s or "WEB_SITE"


def _normalize_adgroup_tp(value: Any, campaign_tp: str = "WEB_SITE") -> str:
    s = str(value or "").strip().upper()
    if s in {"", "기본", "파워링크", "WEB_SITE"}:
        return "WEB_SITE" if campaign_tp == "WEB_SITE" else "SHOPPING"
    if s in {"쇼핑검색", "쇼검", "SHOPPING"}:
        return "SHOPPING"
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
        return reverse[s]
    if s == "SUB_LINK":
        return "SUB_LINKS"
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


def _fetch_adgroups(api_key: str, secret_key: str, cid: str, campaign_id: str):
    res = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": campaign_id})
    if res.status_code != 200:
        return res, []
    rows = [_normalize_adgroup_item(x) for x in (res.json() or [])]
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


def _build_extension_payload(owner_id: str, ext_type: str, data: Dict[str, Any], customer_id: int, position: int | None = None) -> Dict[str, Any]:
    ext_type = _normalize_extension_type(ext_type)
    payload: Dict[str, Any] = {"ownerId": owner_id, "customerId": customer_id, "type": ext_type}
    if position in {1, 2} and ext_type == "HEADLINE":
        payload["priority"] = int(position)
    if ext_type == "HEADLINE":
        payload["adExtension"] = {"headline": str(data.get("headline") or "").strip()}
    elif ext_type in {"DESCRIPTION_EXTRA", "DESCRIPTION"}:
        payload["adExtension"] = {"description": str(data.get("description") or "").strip()}
    elif ext_type == "SUB_LINKS":
        payload["adExtension"] = [
            {"name": str(x.get("name") or "").strip(), "final": str(x.get("final") or "").strip()}
            for x in (data.get("links") or []) if str(x.get("name") or "").strip() and str(x.get("final") or "").strip()
        ]
    return payload


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


def _copy_adgroup_children(api_key, secret_key, cid, old_adg_id, new_adg_id, biz_channel_id, include_keywords=True, include_ads=True, include_extensions=True, include_negatives=True):
    errors = []
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
        if new_kws:
            for i in range(0, len(new_kws), 100):
                batch = new_kws[i:i + 100]
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=batch)
                if res.status_code not in [200, 201]:
                    for item in batch:
                        r_single = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=item)
                        if r_single.status_code not in [200, 201]:
                            errors.append(f"키워드 에러: {r_single.text}")

    if include_ads:
        r_ad = _do_req("GET", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": old_adg_id})
    else:
        r_ad = None
    if r_ad is not None and r_ad.status_code == 200:
        ads = r_ad.json() or []

        def _post_ad(ad):
            item = copy.deepcopy(ad)
            ad_type = item.get("type", "")
            ref_data = item.get("referenceData", {})
            if ad_type in SHOPPING_AD_TYPES:
                item["ad"] = {}
                ref_key = item.get("referenceKey") or ref_data.get("mallProductId") or ref_data.get("id")
                if ref_key:
                    item["referenceKey"] = str(ref_key)
            else:
                item.pop('referenceKey', None)
            for k in ['nccAdId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'referenceData', 'nccQi', 'enable']:
                item.pop(k, None)
            item.update({"nccAdgroupId": str(new_adg_id), "customerId": int(cid)})
            item.setdefault("userLock", False)
            if ad_type in SHOPPING_AD_TYPES:
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id, "isList": "true"}, json_body=[item])
            else:
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id}, json_body=item)
            if res.status_code not in [200, 201]:
                return f"소재 에러: {res.text}"
            return None

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_post_ad, ad) for ad in ads]
            for f in as_completed(futures):
                err_msg = f.result()
                if err_msg:
                    errors.append(err_msg)

    if include_extensions:
        r_ext = _do_req("GET", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": old_adg_id})
    else:
        r_ext = None
    if r_ext is not None and r_ext.status_code == 200:
        for ext in r_ext.json() or []:
            item = copy.deepcopy(ext)
            for k in ['adExtensionId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'referenceKey']:
                item.pop(k, None)
            item.update({"ownerId": str(new_adg_id), "customerId": int(cid)})
            ext_type = item.get("type")
            if biz_channel_id and biz_channel_id not in ["keep", "undefined"] and ext_type in ["SUB_LINKS", "POWER_LINK_IMAGE", "WEBSITE_INFO", "IMAGE_SUB_LINKS"]:
                item["pcChannelId"] = item["mobileChannelId"] = str(biz_channel_id)
            res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": new_adg_id}, json_body=item)
            if res.status_code not in [200, 201] and "4003" not in res.text:
                errors.append(f"확장소재 에러: {res.text}")

    rk_list = []
    if include_negatives:
        r_rk = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{old_adg_id}/restricted-keywords")
    else:
        r_rk = None
    if r_rk is not None and r_rk.status_code == 200 and r_rk.json():
        rk_list = r_rk.json()
    elif include_negatives:
        r_rk2 = _do_req("GET", api_key, secret_key, cid, "/ncc/restricted-keywords", params={"nccAdgroupId": old_adg_id})
        if r_rk2.status_code == 200 and r_rk2.json():
            rk_list = r_rk2.json()
    if rk_list:
        clean_kws = []
        for rk in rk_list:
            if isinstance(rk, dict):
                kw = rk.get("keyword") or rk.get("restrictedKeyword")
                if kw:
                    clean_kws.append(kw)
            elif isinstance(rk, str):
                clean_kws.append(rk)
        if clean_kws:
            post_payload = [{"nccAdgroupId": str(new_adg_id), "customerId": int(cid), "keyword": k} for k in clean_kws]
            r_rk_post = _do_req("POST", api_key, secret_key, cid, "/ncc/restricted-keywords", json_body=post_payload)
            if r_rk_post.status_code not in [200, 201, 204]:
                errors.append(f"제외검색어 에러: {r_rk_post.text}")
    return list(set(errors))


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
    res, rows = _fetch_adgroups(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("campaign_id"))
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
    campaign_tp = _normalize_campaign_tp(d.get("campaign_tp"))
    adgroup_tp = _normalize_adgroup_tp(d.get("adgroup_type"), campaign_tp)
    biz_channel_id = str(d.get("biz_channel_id") or "").strip()
    media_type = str(d.get("media_type") or "ALL").strip().upper()
    media_detail = _normalize_media_detail(d.get("media_detail"))
    use_keyword_plus = d.get("use_keyword_plus")
    use_close_variant = d.get("use_close_variant")
    keyword_plus_weight = d.get("keyword_plus_weight")
    if not campaign_id or not name:
        return jsonify({"error": "캠페인과 광고그룹명은 필수입니다."}), 400
    if campaign_tp == "WEB_SITE" and not biz_channel_id:
        biz_channel_id = _fetch_first_biz_channel_id(d.get("api_key"), d.get("secret_key"), d.get("customer_id"))
        if not biz_channel_id:
            return jsonify({"error": "파워링크 광고그룹은 비즈채널이 필요합니다. 비즈채널을 먼저 선택하거나 생성해주세요."}), 400
    payload = {
        "customerId": int(d.get("customer_id")),
        "nccCampaignId": campaign_id,
        "name": name,
        "adgroupType": adgroup_tp,
        "useDailyBudget": bool(d.get("use_daily_budget", True)),
        "dailyBudget": int(d.get("daily_budget") or 0),
        "bidAmt": int(d.get("bid_amt") or 70),
    }
    if campaign_tp == "WEB_SITE" and biz_channel_id:
        payload["pcChannelId"] = biz_channel_id
        payload["mobileChannelId"] = biz_channel_id
    res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/adgroups", json_body=payload)
    if res.status_code in [200, 201]:
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
            if campaign_tp == "WEB_SITE":
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
        return jsonify({"ok": True, "item": item, "message": message, "warnings": warnings})
    detail = res.text
    try:
        j = res.json()
        detail = j.get("title") or j.get("message") or detail
        if j.get("detail"):
            detail = f"{detail} | {j.get('detail')}"
    except Exception:
        pass
    return jsonify({"error": "광고그룹 생성 실패", "details": detail, "payload": payload}), 400


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
    if not payload.get("adExtension"):
        return {"ok": False, "owner_id": owner_id, "detail": "확장소재 내용이 비어 있습니다."}
    res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
    ok = res is not None and res.status_code in [200, 201]
    return {
        "ok": ok,
        "owner_id": owner_id,
        "detail": (res.text if res is not None else "응답 없음"),
        "item": (res.json() if ok else None),
    }


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
    ext_type = _normalize_extension_type(d.get("type"))
    if not owner_ids or not ext_type:
        return jsonify({"error": "광고그룹과 확장소재 유형은 필수입니다."}), 400
    if ext_type not in {"HEADLINE", "DESCRIPTION_EXTRA", "DESCRIPTION", "SUB_LINKS"}:
        return jsonify({"error": "현재 간편 등록은 추가제목 / 홍보문구 / 추가설명 / 서브링크만 지원합니다."}), 400

    data: Dict[str, Any] = {}
    position = _parse_extension_position(d.get("position"))
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

    results = []
    success = fail = 0
    for owner_id in owner_ids:
        payload = _build_extension_payload(owner_id, ext_type, data, int(d.get("customer_id")), position=position)
        if not payload.get("adExtension"):
            results.append({"ok": False, "owner_id": owner_id, "detail": "확장소재 내용이 비어 있습니다."})
            fail += 1
            continue
        res = _do_req("POST", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
        if res.status_code in [200, 201]:
            success += 1
            item = None
            try:
                item = res.json()
            except Exception:
                item = None
            results.append({"ok": True, "owner_id": owner_id, "detail": "등록 완료", "item": item})
        else:
            fail += 1
            results.append({"ok": False, "owner_id": owner_id, "detail": res.text, "payload": payload})

    if len(owner_ids) == 1:
        r = results[0]
        if r.get("ok"):
            return jsonify({"ok": True, "item": r.get("item"), "message": "확장소재 등록 완료"})
        return jsonify({"error": "확장소재 등록 실패", "details": r.get("detail"), "payload": r.get("payload")}), 400

    return jsonify({
        "ok": fail == 0,
        "total": len(owner_ids),
        "success": success,
        "fail": fail,
        "results": results,
        "message": f"확장소재 등록 완료 (성공 {success}건 / 실패 {fail}건)"
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
    adgroup_id = str(d.get("adgroup_id") or "").strip()
    keywords = [x.strip() for x in str(d.get("keywords") or "").replace(",", "\n").splitlines() if x.strip()]
    if not adgroup_id or not keywords:
        return jsonify({"error": "광고그룹과 제외키워드는 필수입니다."}), 400
    res_existing, existing_rows = _fetch_restricted_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), adgroup_id)
    existing_set = set()
    if res_existing.status_code == 200:
        for item in existing_rows:
            if isinstance(item, dict):
                kw = str(item.get("keyword") or item.get("restrictedKeyword") or "").strip().lower()
                if kw:
                    existing_set.add(kw)
            elif isinstance(item, str):
                existing_set.add(item.strip().lower())
    requested_type = str(d.get("match_type") or "EXACT").strip()
    new_rows = []
    skipped = []
    for kw in keywords:
        if kw.lower() in existing_set:
            skipped.append(kw)
            continue
        new_rows.append({"nccAdgroupId": adgroup_id, "keyword": kw, "type": requested_type})
    success, fail, results = _bulk_create_restricted_keywords(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), new_rows)
    return jsonify({
        "ok": True,
        "total": len(keywords),
        "submitted": len(new_rows),
        "skipped_duplicates": skipped,
        "success": success,
        "fail": fail,
        "results": results,
    })


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
        if r_post.status_code in [200, 201]:
            results["success"] += 1
        else:
            results["fail"] += 1
            all_errors.append(f"[{new_camp['name']}] 생성 실패: {r_post.text}")
    msg = f"캠페인 복사 완료!\n(성공: {results['success']}개, 실패: {results['fail']}개)"
    if all_errors:
        msg += "\n" + "\n".join(all_errors[:5])
    return jsonify({"ok": True, "message": msg})


@app.route("/copy_adgroups_to_target", methods=["POST"])
def copy_adgroups_to_target():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    src_ids = d.get("source_ids", [])
    target_camp_id = d.get("target_campaign_id")
    suffix = d.get("suffix", "_복사본")
    biz_channel_id = d.get("biz_channel_id")
    include_keywords = _boolish(d.get("include_keywords"), True)
    include_ads = _boolish(d.get("include_ads"), True)
    include_extensions = _boolish(d.get("include_extensions"), True)
    include_negatives = _boolish(d.get("include_negatives"), True)
    results, all_errors = {"success": 0, "fail": 0}, []
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{src_id}")
        if r_get.status_code != 200:
            results["fail"] += 1
            continue
        new_adg = _extract_adgroup(r_get.json(), target_camp_id, cid, biz_channel_id)
        new_adg["name"] = str(new_adg.get("name") or "") + suffix
        r_post = _do_req("POST", api_key, secret_key, cid, "/ncc/adgroups", json_body=new_adg)
        if r_post.status_code in [200, 201]:
            results["success"] += 1
            errs = _copy_adgroup_children(api_key, secret_key, cid, src_id, r_post.json().get("nccAdgroupId"), biz_channel_id, include_keywords=include_keywords, include_ads=include_ads, include_extensions=include_extensions, include_negatives=include_negatives)
            all_errors.extend([f"[{new_adg['name']}] {e}" for e in errs])
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


@app.route("/set_campaign_state", methods=["POST"])
def set_campaign_state():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    ids = d.get("ids") or []
    enabled = bool(d.get("enabled", True))
    if not ids:
        return jsonify({"error": "선택된 캠페인이 없습니다."}), 400
    success = fail = 0
    details = []
    for camp_id in ids:
        uri = f"/ncc/campaigns/{str(camp_id).strip()}"
        r_get = _do_req("GET", api_key, secret_key, cid, uri)
        if r_get.status_code != 200:
            fail += 1
            if len(details) < 5:
                details.append(f"[{camp_id}] 조회 실패: {r_get.text}")
            continue
        obj = r_get.json() or {}
        obj["userLock"] = not enabled
        r_put = _do_req("PUT", api_key, secret_key, cid, uri, params={"fields": "userLock"}, json_body=obj)
        if r_put.status_code in [200, 201]:
            success += 1
        else:
            fail += 1
            if len(details) < 5:
                details.append(f"[{camp_id}] 변경 실패: {r_put.text}")
    msg = f"캠페인 {'ON' if enabled else 'OFF'} 완료! (성공: {success} / 실패: {fail})"
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
