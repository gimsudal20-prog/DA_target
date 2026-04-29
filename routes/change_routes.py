# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import Blueprint, jsonify


def _view(service, name):
    def handler():
        try:
            return service.call(name)
        except KeyError as exc:
            return jsonify({"error": str(exc)}), 500
    handler.__name__ = f"change_{name}"
    return handler


def create_change_blueprint(service):
    bp = Blueprint("change_routes", __name__)

    post_routes = [
        ("/rename_adgroups_bulk", "rename_adgroups_bulk"),
        ("/update_media", "update_media"),
        ("/update_powerlink_device_bid_weights", "update_powerlink_device_bid_weights"),
        ("/apply_target_settings_bulk", "apply_target_settings_bulk"),
        ("/update_budget", "update_budget"),
        ("/update_schedule", "update_schedule"),
        ("/update_schedule_campaign_bulk", "update_schedule_campaign_bulk"),
        ("/update_non_search_keyword_exclusion", "update_non_search_keyword_exclusion"),
        ("/preview_keyword_bids_by_search", "preview_keyword_bids_by_search"),
        ("/update_keyword_bids_by_search", "update_keyword_bids_by_search"),
        ("/preview_keyword_bid_weights_by_search", "preview_keyword_bid_weights_by_search"),
        ("/update_keyword_bid_weights_by_search", "update_keyword_bid_weights_by_search"),
        ("/update_keyword_bids", "update_keyword_bids"),
        ("/update_bid_mode_by_scope", "update_bid_mode_by_scope"),
        ("/adjust_keyword_bids_by_threshold", "adjust_keyword_bids_by_threshold"),
        ("/update_keyword_bids_avg_position", "update_keyword_bids_avg_position"),
        ("/set_searched_powerlink_keyword_state", "set_searched_powerlink_keyword_state"),
        ("/set_campaign_state", "set_campaign_state"),
    ]
    for rule, name in post_routes:
        bp.add_url_rule(rule, endpoint=name, view_func=_view(service, name), methods=["POST"])

    for idx, rule in enumerate([
        "/update_adgroup_search_options",
        "/update_adgroup_search_options/",
        "/update_adgroup_options",
        "/update_adgroup_options/",
    ], start=1):
        bp.add_url_rule(
            rule,
            endpoint=f"update_adgroup_options_alias_{idx}",
            view_func=_view(service, "update_adgroup_options"),
            methods=["GET", "POST", "OPTIONS"],
        )

    for idx, (rule, name) in enumerate([
        ("/preview_keyword_avg_position_by_search", "preview_keyword_avg_position_by_search"),
        ("/preview_keyword_avg_position_search", "preview_keyword_avg_position_by_search"),
        ("/preview_powerlink_keyword_avg_position_by_search", "preview_keyword_avg_position_by_search"),
        ("/preview_powerlink_keyword_avg_position_search", "preview_keyword_avg_position_by_search"),
        ("/update_keyword_avg_position_by_search", "update_keyword_avg_position_by_search"),
        ("/update_keyword_avg_position_search", "update_keyword_avg_position_by_search"),
        ("/update_powerlink_keyword_avg_position_by_search", "update_keyword_avg_position_by_search"),
        ("/update_powerlink_keyword_avg_position_search", "update_keyword_avg_position_by_search"),
    ], start=1):
        bp.add_url_rule(rule, endpoint=f"avg_position_alias_{idx}", view_func=_view(service, name), methods=["POST"])

    return bp
