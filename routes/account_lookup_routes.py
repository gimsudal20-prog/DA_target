# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import Blueprint, jsonify, request, send_file


def _json_or_file(result, status):
    if isinstance(result, dict) and result.get("file") is not None:
        return send_file(
            result["file"],
            mimetype=result.get("mimetype"),
            as_attachment=True,
            download_name=result.get("download_name"),
        )
    return jsonify(result), status


def create_account_lookup_blueprint(service):
    bp = Blueprint("account_lookup_routes", __name__)

    @bp.route("/query_account_ads", methods=["POST"])
    def query_account_ads():
        result, status = service.query_ads(request.get_json(silent=True) or {})
        return jsonify(result), status

    @bp.route("/query_account_extensions", methods=["POST"])
    def query_account_extensions():
        result, status = service.query_extensions(request.get_json(silent=True) or {})
        return jsonify(result), status

    @bp.route("/query_account_keywords", methods=["POST"])
    def query_account_keywords():
        result, status = service.query_keywords(request.get_json(silent=True) or {})
        return jsonify(result), status

    @bp.route("/export_account_keywords_excel", methods=["POST"])
    def export_account_keywords_excel():
        return _json_or_file(*service.export_keywords_excel(request.get_json(silent=True) or {}))

    @bp.route("/export_account_ads_excel", methods=["POST"])
    def export_account_ads_excel():
        return _json_or_file(*service.export_ads_excel(request.get_json(silent=True) or {}))

    @bp.route("/export_account_issues_excel", methods=["POST"])
    def export_account_issues_excel():
        return _json_or_file(*service.export_issues_excel(request.get_json(silent=True) or {}))

    @bp.route("/export_account_extensions_excel", methods=["POST"])
    def export_account_extensions_excel():
        return _json_or_file(*service.export_extensions_excel(request.get_json(silent=True) or {}))

    return bp
