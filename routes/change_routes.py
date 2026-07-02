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
        ("/add_restricted_media_ids", "add_restricted_media_ids"),
        ("/copy_restricted_media_settings", "copy_restricted_media_settings"),
        ("/update_adgroup_bid_amt", "update_adgroup_bid_amt"),
        ("/update_powerlink_device_bid_weights", "update_powerlink_device_bid_weights"),
        ("/update_contents_network_bid_amt", "update_contents_network_bid_amt"),
        ("/bulk_update_contents_network_bid_amt", "bulk_update_contents_network_bid_amt"),
        ("/bulk_update_shopping_ad_bids", "bulk_update_shopping_ad_bids"),
        ("/bulk_update_shopping_product_ad_bids", "bulk_update_shopping_product_ad_bids"),
        ("/apply_target_settings_bulk", "apply_target_settings_bulk"),
        ("/get_region_target_options", "get_region_target_options"),
        ("/update_age_targets_bulk", "update_age_targets_bulk"),
        ("/update_region_targets_bulk", "update_region_targets_bulk"),
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
        ("/set_keywords_state_by_scope", "set_keywords_state_by_scope"),
        ("/set_campaign_state", "set_campaign_state"),
        ("/set_adgroup_state_by_scope", "set_adgroup_state_by_scope"),
        ("/set_ads_state_by_scope", "set_ads_state_by_scope"),
        ("/set_ad_extensions_state_by_scope", "set_ad_extensions_state_by_scope"),
        ("/set_asset_state_by_ids", "set_asset_state_by_ids"),
        ("/update_ad_product_name", "update_ad_product_name"),
        ("/update_ad_product_names_bulk", "update_ad_product_names_bulk"),
        ("/bulk_update_ad_product_names_csv", "bulk_update_ad_product_names_csv"),
        ("/update_shopping_ad_bid", "update_shopping_ad_bid"),
    ]
    # Register the main POST routes.
    # Some older local builds or cached HTML can call the restricted-media URLs
    # with a trailing slash or a short legacy path; keep those aliases mapped to
    # the same service so the UI does not fall into Flask's generic 404 page.
    trailing_slash_aliases = {
        "/add_restricted_media_ids",
        "/copy_restricted_media_settings",
    }
    for rule, name in post_routes:
        handler = _view(service, name)
        bp.add_url_rule(rule, endpoint=name, view_func=handler, methods=["POST"])
        if rule in trailing_slash_aliases:
            bp.add_url_rule(
                f"{rule}/",
                endpoint=f"{name}_slash_alias",
                view_func=handler,
                methods=["POST"],
            )

    restricted_media_legacy_aliases = [
        ("/copy_restricted_media", "copy_restricted_media_settings"),
        ("/copy_restricted_media/", "copy_restricted_media_settings"),
        ("/copy_restricted_media_only", "copy_restricted_media_settings"),
        ("/copy_restricted_media_only/", "copy_restricted_media_settings"),
    ]
    for idx, (rule, name) in enumerate(restricted_media_legacy_aliases, start=1):
        bp.add_url_rule(
            rule,
            endpoint=f"{name}_legacy_alias_{idx}",
            view_func=_view(service, name),
            methods=["POST"],
        )

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
