# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import Blueprint, jsonify


def _view(service, name):
    def handler():
        try:
            return service.call(name)
        except KeyError as exc:
            return jsonify({"error": str(exc)}), 500
    handler.__name__ = f"registration_{name}"
    return handler


def create_registration_blueprint(service):
    bp = Blueprint("registration_routes", __name__)
    for rule, name in [
        ("/create_campaign", "create_campaign"),
        ("/create_adgroup_simple", "create_adgroup_simple"),
        ("/create_keywords_simple", "create_keywords_simple"),
        ("/bulk_upload_text_ads", "bulk_upload_text_ads"),
        ("/create_text_ad_simple", "create_text_ad_simple"),
        ("/create_ad_advanced", "create_ad_advanced"),
        ("/create_extension_simple", "create_extension_simple"),
        ("/bulk_upload_headlines", "bulk_upload_headlines"),
        ("/create_shopping_ad_simple", "create_shopping_ad_simple"),
        ("/create_extension_raw", "create_extension_raw"),
        ("/create_restricted_keywords_simple", "create_restricted_keywords_simple"),
        ("/bulk_register", "bulk_register"),
    ]:
        bp.add_url_rule(rule, endpoint=name, view_func=_view(service, name), methods=["POST"])
    return bp
