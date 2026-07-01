# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import Blueprint, jsonify, request


def create_ai_analysis_blueprint(service):
    bp = Blueprint("ai_analysis_routes", __name__)

    @bp.route("/api/ai-analysis/chat", methods=["POST"])
    def ai_analysis_chat():
        result, status = service.analyze(request.get_json(silent=True) or {})
        return jsonify(result), status

    return bp
