# -*- coding: utf-8 -*-
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Optional

from flask import Blueprint, jsonify, request


def create_lookup_blueprint(*, lookup_service: Any, target_object_func: Optional[Callable[..., Any]] = None) -> Blueprint:
    """Create lookup-only routes.

    This is intentionally small for the first route-split step.  The public
    endpoint URLs and response shapes stay identical to the previous app.py
    routes, while app.py keeps ownership of shared services and helpers.
    """

    bp = Blueprint("lookup_routes", __name__)

    @bp.route("/get_campaigns", methods=["POST"])
    def get_campaigns():
        d = request.json or {}
        api_key = d.get("api_key")
        secret_key = d.get("secret_key")
        cid = d.get("customer_id")
        force = bool(d.get("force"))
        res, rows, cached = lookup_service.get_campaigns_cached(api_key, secret_key, cid, force=force)
        if cached or (res is not None and res.status_code == 200):
            return jsonify(rows)
        return jsonify({"error": "캠페인 조회 실패", "details": getattr(res, "text", "")}), 400

    @bp.route("/get_adgroups", methods=["POST"])
    def get_adgroups():
        d = request.json or {}
        api_key = d.get("api_key")
        secret_key = d.get("secret_key")
        cid = d.get("customer_id")
        campaign_id = str(d.get("campaign_id") or "").strip()
        force = bool(d.get("force"))
        if not campaign_id:
            return jsonify([])
        res, rows, cached = lookup_service.get_adgroups_cached(
            api_key,
            secret_key,
            cid,
            campaign_id,
            force=force,
            enrich_media=False,
            target_object_func=target_object_func,
        )
        if cached or (res is not None and res.status_code == 200):
            return jsonify(rows)
        return jsonify({"error": "광고그룹 조회 실패", "details": getattr(res, "text", "")}), 400

    @bp.route("/search_adgroups_global", methods=["POST"])
    def search_adgroups_global():
        d = request.json or {}
        api_key = d.get("api_key")
        secret_key = d.get("secret_key")
        cid = d.get("customer_id")
        query = str(d.get("query") or "").strip()
        match_mode = str(d.get("match_mode") or d.get("adgroup_name_match_mode") or "partial").strip().lower()
        if match_mode not in {"partial", "phrase"}:
            match_mode = "phrase" if match_mode in {"exact", "contains", "구문일치"} else "partial"
        force = bool(d.get("force"))
        try:
            limit = int(d.get("limit") or 50)
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 200))
        if not query:
            return jsonify({"matches": [], "scanned_campaign_count": 0, "warnings": []})

        res_camps, campaigns, cached_camps = lookup_service.get_campaigns_cached(api_key, secret_key, cid, force=force)
        if not (cached_camps or (res_camps is not None and res_camps.status_code == 200)):
            return jsonify({"error": "캠페인 조회 실패", "details": getattr(res_camps, "text", "")}), 400

        def normalize_search_text(value: Any) -> str:
            return " ".join(str(value or "").strip().split()).casefold()

        def adgroup_matches(name: str, adgroup_id: str) -> bool:
            query_key = normalize_search_text(query)
            haystack = f"{normalize_search_text(name)} {normalize_search_text(adgroup_id)}"
            if not query_key:
                return False
            if match_mode == "phrase":
                return query_key in haystack
            terms = [term for term in query_key.split(" ") if term]
            return all(term in haystack for term in terms) if terms else query_key in haystack

        query_key = normalize_search_text(query)
        matches = []
        warnings = []
        campaign_rows = campaigns or []
        max_workers = min(8, max(1, len(campaign_rows)))

        def search_campaign(campaign):
            campaign_id = str((campaign or {}).get("id") or "").strip()
            if not campaign_id:
                return [], None
            res_adg, adgroups, cached_adg = lookup_service.get_adgroups_cached(
                api_key,
                secret_key,
                cid,
                campaign_id,
                force=force,
                enrich_media=False,
                target_object_func=target_object_func,
            )
            if not (cached_adg or (res_adg is not None and res_adg.status_code == 200)):
                return [], f"{campaign.get('name') or campaign_id} 광고그룹 조회 실패"
            campaign_name = str(campaign.get("name") or campaign_id)
            found = []
            for adgroup in adgroups or []:
                name = str((adgroup or {}).get("name") or "")
                adgroup_id = str((adgroup or {}).get("id") or "").strip()
                if not adgroup_matches(name, adgroup_id):
                    continue
                found.append({
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "campaignTp": campaign.get("campaignTp"),
                    "adgroup_id": adgroup_id,
                    "adgroup_name": name,
                    "adgroupType": adgroup.get("adgroupType"),
                    "adgroup": adgroup,
                })
            return found, None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(search_campaign, campaign): campaign for campaign in campaign_rows}
            for future in as_completed(future_map):
                try:
                    found, warning = future.result()
                except Exception as exc:
                    campaign = future_map[future]
                    warning = f"{campaign.get('name') or campaign.get('id') or '캠페인'} 광고그룹 조회 실패: {exc}"
                    found = []
                if warning and len(warnings) < 10:
                    warnings.append(warning)
                if found:
                    matches.extend(found)

        matches.sort(key=lambda row: (
            0 if str(row.get("adgroup_name") or "").casefold() == query_key else 1,
            str(row.get("campaign_name") or "").casefold(),
            str(row.get("adgroup_name") or "").casefold(),
        ))
        return jsonify({
            "matches": matches[:limit],
            "total_match_count": len(matches),
            "scanned_campaign_count": len(campaign_rows),
            "warnings": warnings,
            "match_mode": match_mode,
        })

    @bp.route("/get_biz_channels", methods=["POST"])
    def get_biz_channels():
        d = request.json or {}
        api_key = d.get("api_key")
        secret_key = d.get("secret_key")
        cid = d.get("customer_id")
        force = bool(d.get("force"))
        res, rows, cached = lookup_service.get_channels_cached(api_key, secret_key, cid, force=force)
        if cached or (res is not None and res.status_code == 200):
            return jsonify(rows)
        return jsonify({"error": "비즈채널 조회 실패", "details": getattr(res, "text", "")}), 400

    return bp
