# -*- coding: utf-8 -*-
"""Korean labels and display-format helpers used across UI/export code.

This module is intentionally side-effect free so it can be imported by routes,
services, and export helpers as the backend is gradually split out of app.py.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

CAMPAIGN_TYPE_LABELS = {
    "WEB_SITE": "파워링크",
    "SHOPPING": "쇼핑검색",
    "SHOPPING_BRAND": "쇼핑브랜드",
    "BRAND_SEARCH": "브랜드검색",
    "BRAND_SEARCH_AD": "브랜드검색 소재",
    "CATALOG": "카탈로그",
    "PLACE": "플레이스",
    "PLACE_AD": "플레이스",
    "POWER_CONTENTS": "파워컨텐츠",
    "POWER_CONTENT": "파워컨텐츠",
}

CAMPAIGN_TYPE_COLORS = {
    "WEB_SITE": "blue",
    "SHOPPING": "green",
    "SHOPPING_BRAND": "green",
    "BRAND_SEARCH": "purple",
    "BRAND_SEARCH_AD": "purple",
    "CATALOG": "green",
    "PLACE": "purple",
    "PLACE_AD": "purple",
    "POWER_CONTENTS": "amber",
    "POWER_CONTENT": "amber",
}

ADGROUP_TYPE_LABELS = {
    "WEB_SITE": "파워링크",
    "SHOPPING": "쇼핑검색",
    "SHOPPING_BRAND": "쇼핑브랜드",
    "BRAND_SEARCH": "브랜드검색",
    "BRAND_SEARCH_AD": "브랜드검색 소재",
    "CATALOG": "카탈로그",
}

AD_TYPE_LABELS = {
    "TEXT_45": "기본소재(제목+설명)",
    "RSA_AD": "반응형 검색소재",
    "SHOPPING_PRODUCT_AD": "상품소재",
    "CATALOG_PRODUCT_AD": "카탈로그 상품소재",
    "CATALOG_AD": "카탈로그 소재",
    "SHOPPING_BRAND_AD": "쇼핑브랜드 소재",
    "BRAND_SEARCH": "브랜드검색",
    "BRAND_SEARCH_AD": "브랜드검색 소재",
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

STATUS_LABELS = {
    "ON": "사용",
    "OFF": "중지",
    "PAUSED": "중지",
    "ACTIVE": "사용",
    "ENABLED": "사용",
    "DISABLED": "중지",
}

YES_NO_USE_LABELS = {
    "Y": "사용",
    "YES": "사용",
    "TRUE": "사용",
    "1": "사용",
    "N": "미사용",
    "NO": "미사용",
    "FALSE": "미사용",
    "0": "미사용",
}

TYPE_CODE_LABELS = {
    **CAMPAIGN_TYPE_LABELS,
    **ADGROUP_TYPE_LABELS,
    **AD_TYPE_LABELS,
    **AD_EXTENSION_TYPE_LABELS,
}


def _raw(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _raw(value).upper()


def label_campaign_type(value: Any, default: Optional[Any] = None) -> Any:
    raw = _raw(value)
    if not raw:
        return default if default is not None else "-"
    return CAMPAIGN_TYPE_LABELS.get(raw, CAMPAIGN_TYPE_LABELS.get(raw.upper(), default if default is not None else raw))


def label_campaign_color(value: Any, default: str = "gray") -> str:
    raw = _raw(value)
    return CAMPAIGN_TYPE_COLORS.get(raw, CAMPAIGN_TYPE_COLORS.get(raw.upper(), default))


def label_adgroup_type(value: Any, default: Optional[Any] = None) -> Any:
    raw = _raw(value)
    if not raw:
        return default if default is not None else "-"
    return ADGROUP_TYPE_LABELS.get(
        raw,
        ADGROUP_TYPE_LABELS.get(raw.upper(), CAMPAIGN_TYPE_LABELS.get(raw.upper(), default if default is not None else raw)),
    )


def label_ad_type(value: Any, default: Optional[Any] = None) -> Any:
    raw = _raw(value)
    if not raw:
        return default if default is not None else "-"
    return AD_TYPE_LABELS.get(raw, AD_TYPE_LABELS.get(raw.upper(), default if default is not None else raw))


def label_extension_type(value: Any, default: Optional[Any] = None) -> Any:
    raw = _raw(value)
    if not raw:
        return default if default is not None else "-"
    return AD_EXTENSION_TYPE_LABELS.get(raw, AD_EXTENSION_TYPE_LABELS.get(raw.upper(), default if default is not None else raw))


def label_status(value: Any, default: Optional[Any] = None) -> Any:
    raw_upper = _upper(value)
    if not raw_upper:
        return default if default is not None else value
    return STATUS_LABELS.get(raw_upper, default if default is not None else value)


def label_yes_no_use(value: Any, default: Optional[Any] = None) -> Any:
    raw_upper = _upper(value)
    if not raw_upper:
        return default if default is not None else value
    return YES_NO_USE_LABELS.get(raw_upper, default if default is not None else value)


def format_asset_lookup_excel_value(
    key: str,
    column_label: str,
    value: Any,
    *,
    normalize_ad_type: Optional[Callable[[Any], str]] = None,
    normalize_extension_type: Optional[Callable[[Any], str]] = None,
) -> Any:
    """Return a display-safe Korean value for asset lookup/export workbooks.

    `normalize_ad_type` and `normalize_extension_type` are optional callbacks so
    this utility can reuse the app's existing alias logic without importing app.py.
    """
    raw = _raw(value)
    raw_upper = raw.upper()
    label_text = str(column_label or "")

    if key == "campaignType" or "캠페인유형" in label_text:
        if not raw:
            return value
        return label_campaign_type(raw, default=value)

    if key == "adgroupType" or "광고그룹유형" in label_text:
        if not raw:
            return value
        return label_adgroup_type(raw, default=value)

    if key == "status":
        return label_status(raw, default=value)

    if key == "adUseGroupBidAmt":
        return label_yes_no_use(raw, default=value)

    if key == "type" or "유형" in label_text:
        if not raw:
            return value
        normalized_ad_type = normalize_ad_type(raw) if normalize_ad_type else raw_upper
        ad_label = label_ad_type(normalized_ad_type, default="")
        if ad_label:
            return ad_label
        normalized_ext_type = normalize_extension_type(raw) if normalize_extension_type else raw_upper
        ext_label = label_extension_type(normalized_ext_type, default="")
        if ext_label:
            return ext_label
        known_type_label = TYPE_CODE_LABELS.get(raw_upper)
        if known_type_label:
            return known_type_label
        return value

    # Some Naver API type codes can leak into fallback text columns such as
    # summary/headline when the response does not include a human-readable title.
    # In Excel exports, convert exact type-code cells only, so IDs/URLs/raw JSON are
    # not touched.
    known_type_label = TYPE_CODE_LABELS.get(raw_upper)
    if known_type_label:
        return known_type_label

    return value
