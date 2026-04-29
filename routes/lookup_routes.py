# -*- coding: utf-8 -*-
from __future__ import annotations

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
