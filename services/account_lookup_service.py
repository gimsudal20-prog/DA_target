# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from services.api_response import api_error, api_ok, file_payload
from utils.labels import AD_EXTENSION_TYPE_LABELS


class AccountLookupService:
    """Account-wide asset lookup/export logic split out from app.py.

    A short-lived in-process cache is intentionally limited to account lookup
    routes. It prevents the expensive query -> export flow from collecting the
    same account-wide ads/extensions/keywords twice, while the `_cache_bust`
    payload value lets the frontend invalidate results after mutations.
    """

    def __init__(
        self,
        *,
        normalize_lookup_scope_func: Callable[..., str],
        collect_asset_scope_adgroups_func: Callable[..., Any],
        collect_lookup_ads_func: Callable[..., Any],
        collect_lookup_extensions_func: Callable[..., Any],
        collect_lookup_keywords_func: Callable[..., Any],
        build_asset_lookup_workbook_func: Callable[..., Any],
        workbook_to_bytesio_func: Callable[..., Any],
        xlsx_mime: str,
    ) -> None:
        self.normalize_lookup_scope = normalize_lookup_scope_func
        self.collect_asset_scope_adgroups = collect_asset_scope_adgroups_func
        self.collect_lookup_ads = collect_lookup_ads_func
        self.collect_lookup_extensions = collect_lookup_extensions_func
        self.collect_lookup_keywords = collect_lookup_keywords_func
        self.build_asset_lookup_workbook = build_asset_lookup_workbook_func
        self.workbook_to_bytesio = workbook_to_bytesio_func
        self.xlsx_mime = xlsx_mime
        self._cache_lock = threading.RLock()
        self._cache_ttl_seconds = 90.0
        self._cache_max_items = 24
        self._result_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    @staticmethod
    def _credentials(payload: Dict[str, Any]):
        d = payload or {}
        api_key = str(d.get("api_key") or "").strip()
        secret_key = str(d.get("secret_key") or "").strip()
        cid = str(d.get("customer_id") or "").strip()
        scope = d.get("scope") or d.get("search_scope")
        campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
        adgroup_ids = [str(x or "").strip() for x in (d.get("adgroup_ids") or []) if str(x or "").strip()]
        return api_key, secret_key, cid, scope, campaign_ids, adgroup_ids

    @staticmethod
    def _hash_text(value: Any) -> str:
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()

    def _cache_key(self, kind: str, payload: Dict[str, Any]) -> str:
        api_key, secret_key, cid, scope_raw, campaign_ids, adgroup_ids = self._credentials(payload)
        scope = self.normalize_lookup_scope(scope_raw)
        raw = {
            "kind": kind,
            "api_key_hash": self._hash_text(api_key),
            "secret_key_hash": self._hash_text(secret_key),
            "customer_id": cid,
            "scope": scope,
            "campaign_ids": sorted(campaign_ids),
            "adgroup_ids": sorted(adgroup_ids),
            "cache_bust": str((payload or {}).get("_cache_bust") or "0"),
        }
        return hashlib.sha256(json.dumps(raw, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    def _cache_get(self, kind: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        key = self._cache_key(kind, payload)
        now = time.time()
        with self._cache_lock:
            item = self._result_cache.get(key)
            if not item:
                return None
            ts, value = item
            if now - ts > self._cache_ttl_seconds:
                self._result_cache.pop(key, None)
                return None
            # rows가 많은 계정 단위 조회에서 deepcopy는 체감 렉/메모리 사용량을 크게 키운다.
            # 응답 직렬화 과정에서 rows를 수정하지 않으므로 최상위 dict만 복사한다.
            cached = dict(value)
            cached["cached"] = True
            return cached

    def _cache_set(self, kind: str, payload: Dict[str, Any], value: Dict[str, Any]) -> None:
        key = self._cache_key(kind, payload)
        now = time.time()
        with self._cache_lock:
            stale = [k for k, (ts, _) in self._result_cache.items() if now - ts > self._cache_ttl_seconds]
            for k in stale[:50]:
                self._result_cache.pop(k, None)
            while len(self._result_cache) >= self._cache_max_items:
                oldest_key = min(self._result_cache, key=lambda k: self._result_cache[k][0])
                self._result_cache.pop(oldest_key, None)
            # value는 JSON 응답용 dict라 이후 내부에서 mutate하지 않는다. 대용량 rows deepcopy 방지.
            self._result_cache[key] = (now, dict(value))

    def _collect_contexts(self, payload: Dict[str, Any], fail_label: str):
        api_key, secret_key, cid, scope_raw, campaign_ids, adgroup_ids = self._credentials(payload)
        scope = self.normalize_lookup_scope(scope_raw)
        if not api_key or not secret_key or not cid:
            return None, api_error("API 정보 및 광고주를 선택해주세요."), 400
        res_ctx, contexts, warnings, _ = self.collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids, adgroup_ids)
        if getattr(res_ctx, "status_code", 500) != 200:
            return None, api_error(fail_label, getattr(res_ctx, "text", "")), 400
        return {
            "api_key": api_key,
            "secret_key": secret_key,
            "cid": cid,
            "scope": scope,
            "contexts": contexts,
            "warnings": warnings,
        }, None, 200

    def _collect_ads_result(self, payload: Dict[str, Any], *, use_cache: bool = True):
        if use_cache:
            cached = self._cache_get("ads", payload)
            if cached is not None:
                return cached, 200
        ctx, error, status = self._collect_contexts(payload, "소재 조회 실패")
        if error:
            return error, status
        rows, row_warnings = self.collect_lookup_ads(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        warnings = list(ctx["warnings"] or [])
        warnings.extend(row_warnings)
        result = api_ok(
            scope=ctx["scope"],
            total=len(rows),
            preview_limit=200,
            rows=rows,
            warnings=warnings[:20],
            message=f"소재 {len(rows):,}건 조회 완료",
            cached=False,
        )
        self._cache_set("ads", payload, result)
        return result, 200

    def _collect_extensions_result(self, payload: Dict[str, Any], *, use_cache: bool = True):
        if use_cache:
            cached = self._cache_get("extensions", payload)
            if cached is not None:
                return cached, 200
        ctx, error, status = self._collect_contexts(payload, "확장소재 조회 실패")
        if error:
            return error, status
        warnings = list(ctx["warnings"] or [])
        ad_cache = self._cache_get("ads", payload)
        if ad_cache is not None and isinstance(ad_cache.get("rows"), list):
            ad_rows = ad_cache.get("rows") or []
        else:
            ad_rows, ad_warnings = self.collect_lookup_ads(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
            warnings.extend(ad_warnings)
        rows, row_warnings = self.collect_lookup_extensions(
            ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"], ad_rows=ad_rows
        )
        warnings.extend(row_warnings)
        result = api_ok(
            scope=ctx["scope"],
            total=len(rows),
            preview_limit=200,
            rows=rows,
            warnings=warnings[:20],
            message=f"확장소재 {len(rows):,}건 조회 완료",
            cached=False,
        )
        self._cache_set("extensions", payload, result)
        return result, 200

    def _collect_keywords_result(self, payload: Dict[str, Any], *, use_cache: bool = True):
        if use_cache:
            cached = self._cache_get("keywords", payload)
            if cached is not None:
                return cached, 200
        ctx, error, status = self._collect_contexts(payload, "키워드 조회 실패")
        if error:
            return error, status
        warnings = list(ctx["warnings"] or [])
        rows, row_warnings = self.collect_lookup_keywords(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        warnings.extend(row_warnings)
        result = api_ok(
            scope=ctx["scope"],
            total=len(rows),
            preview_limit=200,
            rows=rows,
            warnings=warnings[:20],
            message=f"등록 키워드 {len(rows):,}건 조회 완료",
            cached=False,
        )
        self._cache_set("keywords", payload, result)
        return result, 200

    def query_ads(self, payload: Dict[str, Any]):
        return self._collect_ads_result(payload, use_cache=True)

    def query_extensions(self, payload: Dict[str, Any]):
        return self._collect_extensions_result(payload, use_cache=True)

    def query_keywords(self, payload: Dict[str, Any]):
        return self._collect_keywords_result(payload, use_cache=True)

    @staticmethod
    def _normalize_extension_type_filter(value: Any) -> str:
        raw = str(value or "ALL").strip()
        if not raw:
            return "ALL"
        upper = raw.upper()
        reverse_labels = {str(label or "").strip().upper(): key for key, label in AD_EXTENSION_TYPE_LABELS.items()}
        alias = {
            "ALL": "ALL",
            "전체": "ALL",
            "HEADLINE": "HEADLINE",
            "추가 제목": "HEADLINE",
            "SUB_LINK": "SUB_LINKS",
            "SUB_LINKS": "SUB_LINKS",
            "서브링크": "SUB_LINKS",
            "IMAGE_SUB_LINK": "IMAGE_SUB_LINKS",
            "IMAGE_SUB_LINKS": "IMAGE_SUB_LINKS",
            "이미지 서브링크": "IMAGE_SUB_LINKS",
            "DESCRIPTION": "DESCRIPTION",
            "홍보문구": "DESCRIPTION",
            "홍보 문구": "DESCRIPTION",
            "DESCRIPTION_EXTRA": "DESCRIPTION_EXTRA",
            "추가설명": "DESCRIPTION_EXTRA",
            "추가 설명": "DESCRIPTION_EXTRA",
            "추가 설명문구": "DESCRIPTION_EXTRA",
            "설명 확장문구": "DESCRIPTION_EXTRA",
            "SHOPPING_PROMO_TEXT": "PROMOTION",
            "SHOPPING_EXTRA": "SHOPPING_EXTRA",
            "쇼핑상품부가정보": "SHOPPING_EXTRA",
            "POWER_LINK_IMAGE": "POWER_LINK_IMAGE",
            "파워링크 이미지": "POWER_LINK_IMAGE",
            "WEBSITE_INFO": "WEBSITE_INFO",
            "웹사이트 정보": "WEBSITE_INFO",
        }
        return alias.get(upper, reverse_labels.get(upper, upper))

    @classmethod
    def _extension_filter_label(cls, value: Any) -> str:
        normalized = cls._normalize_extension_type_filter(value)
        if normalized == "ALL":
            return "전체"
        return AD_EXTENSION_TYPE_LABELS.get(normalized, normalized)

    @staticmethod
    def _split_image_ids(value: Any) -> List[str]:
        if value is None:
            return []
        raw = str(value or "").strip()
        if not raw:
            return []
        tokens = []
        for part in raw.replace("\n", ",").replace(";", ",").split(","):
            text = str(part or "").strip()
            if text and text not in tokens:
                tokens.append(text)
        return tokens

    @classmethod
    def _row_extension_type(cls, row: Dict[str, Any]) -> str:
        raw = str((row or {}).get("type") or (row or {}).get("adExtensionType") or "").strip()
        normalized = cls._normalize_extension_type_filter(raw)
        image_ids = cls._split_image_ids((row or {}).get("imageId"))
        # Naver responses can sometimes expose image sublinks as SUB_LINKS with
        # image fields. Treat those rows as IMAGE_SUB_LINKS for filtering/export.
        if normalized == "SUB_LINKS" and image_ids:
            return "IMAGE_SUB_LINKS"
        return normalized

    @classmethod
    def _prepare_extension_export_rows(cls, rows: List[Dict[str, Any]], extension_type: Any) -> List[Dict[str, Any]]:
        target = cls._normalize_extension_type_filter(extension_type)
        prepared: List[Dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            resolved_type = cls._row_extension_type(row)
            if target != "ALL" and resolved_type != target:
                continue
            image_ids = cls._split_image_ids(row.get("imageId"))
            row_copy = dict(row)
            row_copy["resolvedExtensionType"] = resolved_type
            row_copy["extensionTypeLabel"] = AD_EXTENSION_TYPE_LABELS.get(resolved_type, row.get("type") or resolved_type)
            row_copy["imageIdCount"] = len(image_ids) if image_ids else ""
            row_copy["imageIdDetail"] = "\n".join([f"이미지 {idx}: {image_id}" for idx, image_id in enumerate(image_ids, start=1)])
            row_copy.setdefault("periodSetting", "설정안함")
            row_copy.setdefault("periodStartDate", "")
            row_copy.setdefault("periodEndDate", "")
            prepared.append(row_copy)
        return prepared

    def _export_result(self, rows: List[Dict[str, Any]], title: str, scope: str, columns, filename_prefix: str, *, scope_suffix: str | None = None):
        if scope == "adgroup":
            scope_label = "선택 광고그룹만"
        elif scope == "campaign":
            scope_label = "선택 캠페인만"
        else:
            scope_label = "계정 전체"
        if scope_suffix:
            scope_label = f"{scope_label} · {scope_suffix}"
        wb = self.build_asset_lookup_workbook(rows, title, scope_label, columns)
        output = self.workbook_to_bytesio(wb)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        safe_suffix = ""
        if scope_suffix:
            safe_suffix = "_" + "_".join(str(scope_suffix).replace("/", " ").split())
        return file_payload(
            output,
            mimetype=self.xlsx_mime,
            download_name=f"{filename_prefix}_{scope}{safe_suffix}_{stamp}.xlsx",
        ), 200

    @staticmethod
    def _split_summary_title_description(summary: Any) -> Tuple[str, str]:
        text = str(summary or "").strip()
        if not text:
            return "", ""
        for sep in [" / ", "\n", " | "]:
            if sep in text:
                left, right = text.split(sep, 1)
                return left.strip(), right.strip()
        return text, ""

    @staticmethod
    def _deep_first_text_by_keys(value: Any, keys: List[str]) -> str:
        if isinstance(value, dict):
            for key in keys:
                raw = value.get(key)
                if isinstance(raw, (str, int, float)) and str(raw).strip():
                    return str(raw).strip()
            for nested in value.values():
                found = AccountLookupService._deep_first_text_by_keys(nested, keys)
                if found:
                    return found
        elif isinstance(value, list):
            for item in value:
                found = AccountLookupService._deep_first_text_by_keys(item, keys)
                if found:
                    return found
        return ""

    def _prepare_ad_export_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        prepared: List[Dict[str, Any]] = []
        for row in rows or []:
            item = dict(row or {})
            fallback_title, fallback_desc = self._split_summary_title_description(item.get("summary"))
            headline = str(
                item.get("headline")
                or item.get("title")
                or item.get("productName")
                or self._deep_first_text_by_keys(item.get("ad"), ["headline", "title", "productName", "name"])
                or fallback_title
                or ""
            ).strip()
            description = str(
                item.get("description")
                or item.get("desc")
                or item.get("description1")
                or item.get("description2")
                or item.get("longDescription")
                or self._deep_first_text_by_keys(item.get("ad"), ["description", "desc", "description1", "description2", "longDescription", "adDescription"])
                or fallback_desc
                or ""
            ).strip()
            item["headline"] = headline
            item["description"] = description
            item["productName"] = str(item.get("productName") or self._deep_first_text_by_keys(item.get("ad"), ["productName", "mallProductName"]) or "").strip()
            item["pcFinalUrl"] = str(item.get("pcFinalUrl") or self._deep_first_text_by_keys(item.get("ad"), ["pcFinalUrl", "finalUrl", "landingUrl", "url"]) or "").strip()
            item["mobileFinalUrl"] = str(item.get("mobileFinalUrl") or self._deep_first_text_by_keys(item.get("ad"), ["mobileFinalUrl", "mobileUrl", "mobileLandingUrl"]) or item.get("pcFinalUrl") or "").strip()
            if not item.get("summary"):
                item["summary"] = " / ".join([v for v in [headline, description] if v])
            prepared.append(item)
        return prepared

    def export_keywords_excel(self, payload: Dict[str, Any]):
        result, status = self._collect_keywords_result(payload, use_cache=True)
        if status != 200:
            return result, status
        rows = result.get("rows") or []
        if not rows:
            return api_error("내보낼 등록 키워드가 없습니다."), 400
        return self._export_result(rows, "계정 등록 키워드 조회", result.get("scope") or "account", [
            ("campaignName", "캠페인명"), ("adgroupName", "광고그룹명"), ("keyword", "키워드"), ("status", "상태"),
            ("bidAmt", "입찰가"), ("useGroupBidAmt", "그룹입찰가사용"), ("matchType", "매치유형"),
            ("keywordId", "키워드 ID"), ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
        ], "account_keywords")

    def export_ads_excel(self, payload: Dict[str, Any]):
        result, status = self._collect_ads_result(payload, use_cache=True)
        if status != 200:
            return result, status
        rows = self._prepare_ad_export_rows(result.get("rows") or [])
        if not rows:
            return api_error("내보낼 소재가 없습니다."), 400
        return self._export_result(rows, "계정 등록 소재 조회", result.get("scope") or "account", [
            ("campaignType", "캠페인유형"), ("campaignName", "캠페인명"), ("adgroupType", "광고그룹유형"), ("adgroupName", "광고그룹명"),
            ("type", "소재유형"), ("status", "상태"),
            ("headline", "소재 제목"), ("description", "소재 설명"), ("productName", "상품명"),
            ("pcFinalUrl", "PC 연결 URL"), ("mobileFinalUrl", "모바일 연결 URL"),
            ("summary", "요약"), ("effectiveBidAmt", "적용입찰가"), ("adBidAmt", "소재입찰가"), ("adUseGroupBidAmt", "그룹입찰가사용"), ("adgroupBidAmt", "광고그룹입찰가"),
            ("adId", "소재 ID"), ("referenceKey", "참조키"), ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
        ], "account_ads")

    @classmethod
    def _extension_export_columns(cls, extension_type: Any, rows: List[Dict[str, Any]] | None = None):
        """Return export columns suited to the selected extension type.

        Type-specific extension downloads should not expose unrelated image
        columns. For example, HEADLINE(추가 제목) exports do not need image ID
        columns, while IMAGE_SUB_LINKS and POWER_LINK_IMAGE do.
        """
        target = cls._normalize_extension_type_filter(extension_type)
        if target == "IMAGE_SUB_LINKS":
            columns = [
                ("campaignName", "캠페인 이름"),
                ("adgroupName", "광고그룹 이름"),
                ("adgroupId", "광고그룹 ID"),
                ("adExtensionId", "확장소재 ID"),
                ("periodSetting", "기간 설정"),
                ("periodStartDate", "기간 시작일"),
                ("periodEndDate", "기간 종료일"),
            ]
            for idx in range(1, 4):
                columns.extend([
                    (f"link{idx}ImageId", f"링크{idx} 이미지ID"),
                    (f"link{idx}Name", f"링크{idx} 이름"),
                    (f"link{idx}Url", f"링크{idx} URL"),
                ])
            return columns

        base_columns = [
            ("campaignType", "캠페인유형"), ("campaignName", "캠페인명"),
            ("adgroupType", "광고그룹유형"), ("adgroupName", "광고그룹명"),
            ("ownerScope", "적용대상"), ("extensionTypeLabel", "확장소재유형"), ("status", "상태"),
            ("summary", "요약"), ("adExtensionId", "확장소재 ID"),
        ]
        tail_columns = [("ownerId", "owner ID"), ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID")]

        image_types = {"IMAGE_SUB_LINKS", "POWER_LINK_IMAGE"}
        has_image_data = any(cls._split_image_ids((row or {}).get("imageId")) for row in (rows or []))
        include_image_columns = target in image_types or (target == "ALL" and has_image_data)
        if include_image_columns:
            base_columns.extend([
                ("imageIdCount", "이미지 ID 수"),
                ("imageIdDetail", "이미지별 ID"),
                ("imageId", "이미지 ID 원문"),
            ])
        return base_columns + tail_columns

    def export_extensions_excel(self, payload: Dict[str, Any]):
        result, status = self._collect_extensions_result(payload, use_cache=True)
        if status != 200:
            return result, status
        rows = result.get("rows") or []
        if not rows:
            return api_error("내보낼 확장소재가 없습니다."), 400
        extension_type = (payload or {}).get("extension_type") or (payload or {}).get("extensionType") or "ALL"
        filtered_rows = self._prepare_extension_export_rows(rows, extension_type)
        if not filtered_rows:
            label = self._extension_filter_label(extension_type)
            return api_error(f"내보낼 {label} 확장소재가 없습니다."), 400
        label = self._extension_filter_label(extension_type)
        title = "계정 등록 확장소재 조회" if label == "전체" else f"계정 등록 확장소재 조회 - {label}"
        filename_prefix = "account_extensions" if label == "전체" else f"account_extensions_{self._normalize_extension_type_filter(extension_type).lower()}"
        columns = self._extension_export_columns(extension_type, filtered_rows)
        return self._export_result(filtered_rows, title, result.get("scope") or "account", columns, filename_prefix, scope_suffix=f"확장소재유형 {label}")
