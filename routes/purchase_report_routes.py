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


def create_purchase_report_blueprint(service):
    bp = Blueprint("purchase_report_routes", __name__)

    @bp.route("/get_purchase_report_current", methods=["POST"])
    def get_purchase_report_current():
        result, status = service.collect_current(request.get_json(silent=True) or {})
        return jsonify(result), status

    @bp.route("/export_purchase_report_current_excel", methods=["POST"])
    def export_purchase_report_current_excel():
        return _json_or_file(*service.export_current_excel(request.get_json(silent=True) or {}))

    return bp
