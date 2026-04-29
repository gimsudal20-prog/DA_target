# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import Blueprint, jsonify, request


def create_detail_lookup_blueprint(service):
    bp = Blueprint("detail_lookup_routes", __name__)

    @bp.route("/get_keywords", methods=["POST"])
    def get_keywords():
        payload, status = service.get_keywords(request.get_json(silent=True) or {})
        return jsonify(payload), status

    @bp.route("/get_ads", methods=["POST"])
    def get_ads():
        payload, status = service.get_ads(request.get_json(silent=True) or {})
        return jsonify(payload), status

    @bp.route("/get_ad_extensions", methods=["POST"])
    def get_ad_extensions():
        payload, status = service.get_ad_extensions(request.get_json(silent=True) or {})
        return jsonify(payload), status

    @bp.route("/get_restricted_keywords", methods=["POST"])
    def get_restricted_keywords():
        payload, status = service.get_restricted_keywords(request.get_json(silent=True) or {})
        return jsonify(payload), status

    return bp
