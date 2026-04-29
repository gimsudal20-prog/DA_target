# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from typing import Any, Callable, Dict, List


class AccountLookupService:
    """Account-wide asset lookup/export logic split out from app.py."""

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

    @staticmethod
    def _credentials(payload: Dict[str, Any]):
        d = payload or {}
        api_key = str(d.get("api_key") or "").strip()
        secret_key = str(d.get("secret_key") or "").strip()
        cid = str(d.get("customer_id") or "").strip()
        scope = d.get("scope") or d.get("search_scope")
        campaign_ids = [str(x or "").strip() for x in (d.get("campaign_ids") or []) if str(x or "").strip()]
        return api_key, secret_key, cid, scope, campaign_ids

    def _collect_contexts(self, payload: Dict[str, Any], fail_label: str):
        api_key, secret_key, cid, scope_raw, campaign_ids = self._credentials(payload)
        scope = self.normalize_lookup_scope(scope_raw)
        if not api_key or not secret_key or not cid:
            return None, {"error": "API 정보 및 광고주를 선택해주세요."}, 400
        res_ctx, contexts, warnings, _ = self.collect_asset_scope_adgroups(api_key, secret_key, cid, scope, campaign_ids)
        if getattr(res_ctx, "status_code", 500) != 200:
            return None, {"error": fail_label, "details": getattr(res_ctx, "text", "")}, 400
        return {
            "api_key": api_key,
            "secret_key": secret_key,
            "cid": cid,
            "scope": scope,
            "contexts": contexts,
            "warnings": warnings,
        }, None, 200

    def query_ads(self, payload: Dict[str, Any]):
        ctx, error, status = self._collect_contexts(payload, "소재 조회 실패")
        if error:
            return error, status
        rows, row_warnings = self.collect_lookup_ads(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        warnings = list(ctx["warnings"] or [])
        warnings.extend(row_warnings)
        return {
            "ok": True,
            "scope": ctx["scope"],
            "total": len(rows),
            "preview_limit": 200,
            "rows": rows,
            "warnings": warnings[:20],
            "message": f"소재 {len(rows):,}건 조회 완료",
        }, 200

    def query_extensions(self, payload: Dict[str, Any]):
        ctx, error, status = self._collect_contexts(payload, "확장소재 조회 실패")
        if error:
            return error, status
        warnings = list(ctx["warnings"] or [])
        ad_rows, ad_warnings = self.collect_lookup_ads(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        warnings.extend(ad_warnings)
        rows, row_warnings = self.collect_lookup_extensions(
            ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"], ad_rows=ad_rows
        )
        warnings.extend(row_warnings)
        return {
            "ok": True,
            "scope": ctx["scope"],
            "total": len(rows),
            "preview_limit": 200,
            "rows": rows,
            "warnings": warnings[:20],
            "message": f"확장소재 {len(rows):,}건 조회 완료",
        }, 200

    def query_keywords(self, payload: Dict[str, Any]):
        ctx, error, status = self._collect_contexts(payload, "키워드 조회 실패")
        if error:
            return error, status
        warnings = list(ctx["warnings"] or [])
        rows, row_warnings = self.collect_lookup_keywords(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        warnings.extend(row_warnings)
        return {
            "ok": True,
            "scope": ctx["scope"],
            "total": len(rows),
            "preview_limit": 200,
            "rows": rows,
            "warnings": warnings[:20],
            "message": f"등록 키워드 {len(rows):,}건 조회 완료",
        }, 200

    def _export_result(self, rows: List[Dict[str, Any]], title: str, scope: str, columns, filename_prefix: str):
        scope_label = "선택 캠페인만" if scope == "campaign" else "계정 전체"
        wb = self.build_asset_lookup_workbook(rows, title, scope_label, columns)
        output = self.workbook_to_bytesio(wb)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        return {
            "file": output,
            "mimetype": self.xlsx_mime,
            "download_name": f"{filename_prefix}_{scope}_{stamp}.xlsx",
        }, 200

    def export_keywords_excel(self, payload: Dict[str, Any]):
        ctx, error, status = self._collect_contexts(payload, "키워드 엑셀 다운로드 실패")
        if error:
            return error, status
        rows, row_warnings = self.collect_lookup_keywords(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        if not rows:
            return {"error": "내보낼 등록 키워드가 없습니다."}, 400
        return self._export_result(rows, "계정 등록 키워드 조회", ctx["scope"], [
            ("campaignName", "캠페인명"), ("adgroupName", "광고그룹명"), ("keyword", "키워드"), ("status", "상태"),
            ("bidAmt", "입찰가"), ("useGroupBidAmt", "그룹입찰가사용"), ("matchType", "매치유형"),
            ("keywordId", "키워드 ID"), ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
        ], "account_keywords")

    def export_ads_excel(self, payload: Dict[str, Any]):
        ctx, error, status = self._collect_contexts(payload, "소재 엑셀 다운로드 실패")
        if error:
            return error, status
        rows, row_warnings = self.collect_lookup_ads(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        if not rows:
            return {"error": "내보낼 소재가 없습니다."}, 400
        return self._export_result(rows, "계정 등록 소재 조회", ctx["scope"], [
            ("campaignType", "캠페인유형"), ("campaignName", "캠페인명"), ("adgroupType", "광고그룹유형"), ("adgroupName", "광고그룹명"),
            ("type", "소재유형"), ("status", "상태"),
            ("summary", "요약"), ("effectiveBidAmt", "적용입찰가"), ("adBidAmt", "소재입찰가"), ("adUseGroupBidAmt", "그룹입찰가사용"), ("adgroupBidAmt", "광고그룹입찰가"), ("adId", "소재 ID"), ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
        ], "account_ads")

    def export_extensions_excel(self, payload: Dict[str, Any]):
        ctx, error, status = self._collect_contexts(payload, "확장소재 엑셀 다운로드 실패")
        if error:
            return error, status
        ad_rows, ad_warnings = self.collect_lookup_ads(ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"])
        rows, row_warnings = self.collect_lookup_extensions(
            ctx["api_key"], ctx["secret_key"], ctx["cid"], ctx["contexts"], ad_rows=ad_rows
        )
        if not rows:
            return {"error": "내보낼 확장소재가 없습니다."}, 400
        return self._export_result(rows, "계정 등록 확장소재 조회", ctx["scope"], [
            ("campaignType", "캠페인유형"), ("campaignName", "캠페인명"), ("adgroupType", "광고그룹유형"), ("adgroupName", "광고그룹명"),
            ("ownerScope", "적용대상"), ("adExtensionId", "확장소재 ID"),
            ("imageId", "이미지 ID"), ("type", "확장소재유형"), ("status", "상태"), ("summary", "요약"), ("ownerId", "owner ID"),
            ("campaignId", "캠페인 ID"), ("adgroupId", "광고그룹 ID"),
        ], "account_extensions")
