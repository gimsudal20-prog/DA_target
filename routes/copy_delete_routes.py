# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import Blueprint, jsonify


def _view(service, name):
    def handler():
        try:
            return service.call(name)
        except KeyError as exc:
            return jsonify({"error": str(exc)}), 500
    handler.__name__ = f"copy_delete_{name}"
    return handler


def create_copy_delete_blueprint(service):
    bp = Blueprint("copy_delete_routes", __name__)
    for rule, name in [
        ("/copy_entities_to_adgroups", "copy_entities_to_adgroups"),
        ("/copy_campaigns", "copy_campaigns"),
        ("/copy_adgroups_to_target", "copy_adgroups_to_target"),
        ("/bulk_delete_by_parent", "bulk_delete_by_parent"),
        ("/bulk_delete", "bulk_delete"),
        ("/delete_selected", "delete_selected"),
    ]:
        bp.add_url_rule(rule, endpoint=name, view_func=_view(service, name), methods=["POST"])
    return bp
