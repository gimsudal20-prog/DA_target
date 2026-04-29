# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


ResponseRows = Tuple[Any, List[Dict[str, Any]]]


class DetailLookupService:
    """Selected adgroup detail lookup logic split out from app.py.

    Keeps the existing URL contracts intact:
    - /get_keywords supports preview_limit + keyword_search metadata response
    - /get_ads, /get_ad_extensions, /get_restricted_keywords return raw row arrays
    """

    def __init__(
        self,
        *,
        fetch_keywords_func: Callable[..., ResponseRows],
        fetch_ads_func: Callable[..., ResponseRows],
        fetch_extensions_func: Callable[..., ResponseRows],
        fetch_restricted_keywords_func: Callable[..., ResponseRows],
        keyword_matches_search_func: Callable[..., bool],
    ) -> None:
        self.fetch_keywords = fetch_keywords_func
        self.fetch_ads = fetch_ads_func
        self.fetch_extensions = fetch_extensions_func
        self.fetch_restricted_keywords = fetch_restricted_keywords_func
        self.keyword_matches_search = keyword_matches_search_func

    def get_keywords(self, payload: Dict[str, Any]):
        d = payload or {}
        res, rows = self.fetch_keywords(
            d.get("api_key"),
            d.get("secret_key"),
            d.get("customer_id"),
            d.get("adgroup_id"),
        )
        if getattr(res, "status_code", 500) != 200:
            return {"error": "키워드 조회 실패", "details": getattr(res, "text", "")}, 400

        base_rows = rows or []
        search_text = str(d.get("keyword_search") or "").strip()
        match_mode = str(d.get("keyword_match_mode") or "partial").strip().lower()
        exact_match = match_mode == "exact"

        if search_text:
            filtered_rows = [
                row for row in base_rows
                if self.keyword_matches_search(
                    (row or {}).get("keyword")
                    or (row or {}).get("keywordNm")
                    or (row or {}).get("keywordName")
                    or (row or {}).get("name")
                    or "",
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
            return {
                "rows": preview_rows,
                "total_count": total_count,
                "total_unfiltered_count": len(base_rows or []),
                "truncated": total_count > len(preview_rows),
                "preview_limit": preview_limit,
                "keyword_search": search_text,
                "keyword_match_mode": "exact" if exact_match else "partial",
            }, 200

        return filtered_rows, 200

    def get_ads(self, payload: Dict[str, Any]):
        d = payload or {}
        res, rows = self.fetch_ads(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_id"))
        if getattr(res, "status_code", 500) == 200:
            return rows, 200
        return {"error": "소재 조회 실패", "details": getattr(res, "text", "")}, 400

    def get_ad_extensions(self, payload: Dict[str, Any]):
        d = payload or {}
        res, rows = self.fetch_extensions(d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("owner_id"))
        if getattr(res, "status_code", 500) == 200:
            return rows, 200
        return {"error": "확장소재 조회 실패", "details": getattr(res, "text", "")}, 400

    def get_restricted_keywords(self, payload: Dict[str, Any]):
        d = payload or {}
        res, rows = self.fetch_restricted_keywords(
            d.get("api_key"),
            d.get("secret_key"),
            d.get("customer_id"),
            d.get("adgroup_id"),
        )
        if getattr(res, "status_code", 500) == 200:
            return rows, 200
        return {"error": "제외키워드 조회 실패", "details": getattr(res, "text", "")}, 400
