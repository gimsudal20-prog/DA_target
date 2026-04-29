# -*- coding: utf-8 -*-
"""Static route split audit for the Naver Ads Control backend.

Run from the project root:
    python scripts/route_split_audit.py

This script intentionally does not import Flask or start the app. It scans
source text only, so it can run in lightweight local/GitHub Action checks.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PY = PROJECT_ROOT / "app.py"
ROUTES_DIR = PROJECT_ROOT / "routes"

ROUTE_GROUPS: Dict[str, List[str]] = {
    "stage1_lookup": ["/get_campaigns", "/get_adgroups", "/get_biz_channels"],
    "stage2_detail_lookup": ["/get_keywords", "/get_ads", "/get_ad_extensions", "/get_restricted_keywords"],
    "stage3_account_lookup": [
        "/query_account_ads", "/query_account_extensions", "/query_account_keywords",
        "/export_account_ads_excel", "/export_account_extensions_excel", "/export_account_keywords_excel",
    ],
    "stage4_registration": [
        "/create_campaign", "/create_adgroup_simple", "/create_keywords_simple", "/bulk_upload_text_ads",
        "/create_text_ad_simple", "/create_ad_advanced", "/create_extension_simple", "/bulk_upload_headlines",
        "/create_shopping_ad_simple", "/create_extension_raw", "/create_restricted_keywords_simple", "/bulk_register",
    ],
    "stage5_change": [
        "/rename_adgroups_bulk", "/update_media", "/update_powerlink_device_bid_weights", "/apply_target_settings_bulk",
        "/update_budget", "/update_schedule", "/update_schedule_campaign_bulk", "/update_non_search_keyword_exclusion",
        "/preview_keyword_bids_by_search", "/update_keyword_bids_by_search",
        "/preview_keyword_bid_weights_by_search", "/update_keyword_bid_weights_by_search", "/update_keyword_bids",
        "/update_bid_mode_by_scope", "/adjust_keyword_bids_by_threshold", "/update_keyword_bids_avg_position",
        "/set_searched_powerlink_keyword_state", "/set_campaign_state", "/update_adgroup_search_options",
        "/update_adgroup_options", "/preview_keyword_avg_position_by_search", "/update_keyword_avg_position_by_search",
    ],
    "stage6_copy_delete": [
        "/copy_entities_to_adgroups", "/copy_campaigns", "/copy_adgroups_to_target",
        "/bulk_delete_by_parent", "/bulk_delete", "/delete_selected",
    ],
}

EXPECTED_ROUTE_FILES: Dict[str, str] = {
    "/get_campaigns": "lookup_routes.py",
    "/get_adgroups": "lookup_routes.py",
    "/get_biz_channels": "lookup_routes.py",
    "/get_keywords": "detail_lookup_routes.py",
    "/get_ads": "detail_lookup_routes.py",
    "/get_ad_extensions": "detail_lookup_routes.py",
    "/get_restricted_keywords": "detail_lookup_routes.py",
    "/query_account_ads": "account_lookup_routes.py",
    "/query_account_extensions": "account_lookup_routes.py",
    "/query_account_keywords": "account_lookup_routes.py",
    "/export_account_ads_excel": "account_lookup_routes.py",
    "/export_account_extensions_excel": "account_lookup_routes.py",
    "/export_account_keywords_excel": "account_lookup_routes.py",
}
for route in ROUTE_GROUPS["stage4_registration"]:
    EXPECTED_ROUTE_FILES[route] = "registration_routes.py"
for route in ROUTE_GROUPS["stage5_change"]:
    EXPECTED_ROUTE_FILES[route] = "change_routes.py"
for route in ROUTE_GROUPS["stage6_copy_delete"]:
    EXPECTED_ROUTE_FILES[route] = "copy_delete_routes.py"

# Handler names that were moved out of app.py. If they reappear in app.py, the
# split structure is drifting back toward a monolith even when @app.route is not
# duplicated yet.
SPLIT_HANDLER_NAMES: Dict[str, str] = {
    "get_campaigns": "lookup_routes.py",
    "get_adgroups": "lookup_routes.py",
    "get_biz_channels": "lookup_routes.py",
    "get_keywords": "detail_lookup_routes.py",
    "get_ads": "detail_lookup_routes.py",
    "get_ad_extensions": "detail_lookup_routes.py",
    "get_restricted_keywords": "detail_lookup_routes.py",
    "query_account_ads": "account_lookup_routes.py",
    "query_account_extensions": "account_lookup_routes.py",
    "query_account_keywords": "account_lookup_routes.py",
    "export_account_ads_excel": "account_lookup_routes.py",
    "export_account_extensions_excel": "account_lookup_routes.py",
    "export_account_keywords_excel": "account_lookup_routes.py",
}

APP_ROUTE_RE = re.compile(r"@app\.route\(\s*['\"](?P<route>/[^'\"]+)['\"]")
LITERAL_ROUTE_RE = re.compile(r"['\"](?P<route>/[^'\"]+)['\"]")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def scan_app_routes() -> Dict[str, List[int]]:
    routes: Dict[str, List[int]] = {}
    for lineno, line in enumerate(read_text(APP_PY).splitlines(), start=1):
        match = APP_ROUTE_RE.search(line)
        if match:
            routes.setdefault(match.group("route"), []).append(lineno)
    return routes


def scan_app_split_handler_defs() -> Dict[str, int]:
    found: Dict[str, int] = {}
    handler_names = set(SPLIT_HANDLER_NAMES)
    for lineno, line in enumerate(read_text(APP_PY).splitlines(), start=1):
        match = re.match(r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", line)
        if match and match.group(1) in handler_names:
            found[match.group(1)] = lineno
    return found


def scan_route_files() -> Dict[str, List[Tuple[str, int]]]:
    expected = set(EXPECTED_ROUTE_FILES)
    hits: Dict[str, List[Tuple[str, int]]] = {}
    for path in sorted(ROUTES_DIR.glob("*_routes.py")):
        for lineno, line in enumerate(read_text(path).splitlines(), start=1):
            for match in LITERAL_ROUTE_RE.finditer(line):
                route = match.group("route")
                if route in expected:
                    hits.setdefault(route, []).append((path.name, lineno))
    return hits


def route_group_for(route: str) -> str:
    for group, routes in ROUTE_GROUPS.items():
        if route in routes:
            return group
    return "unknown"


def main() -> int:
    expected_routes: Set[str] = {route for group in ROUTE_GROUPS.values() for route in group}
    route_hits = scan_route_files()
    app_routes = scan_app_routes()
    app_split_handlers = scan_app_split_handler_defs()

    missing = sorted(route for route in expected_routes if route not in route_hits)
    duplicated_in_blueprints = {
        route: hits for route, hits in route_hits.items()
        if route in expected_routes and len(hits) != 1
    }
    registered_in_unexpected_file = {
        route: hits for route, hits in route_hits.items()
        if route in EXPECTED_ROUTE_FILES and any(filename != EXPECTED_ROUTE_FILES[route] for filename, _ in hits)
    }
    still_registered_in_app_py = {
        route: lines for route, lines in app_routes.items()
        if route in expected_routes
    }
    legacy_handlers_in_app_py = {
        name: {"line": line, "expected_file": SPLIT_HANDLER_NAMES[name]}
        for name, line in sorted(app_split_handlers.items(), key=lambda item: item[1])
    }

    route_map_by_file: Dict[str, List[str]] = {}
    for route, expected_file in sorted(EXPECTED_ROUTE_FILES.items(), key=lambda item: (item[1], item[0])):
        route_map_by_file.setdefault(expected_file, []).append(route)

    report = {
        "ok": not (
            missing
            or duplicated_in_blueprints
            or registered_in_unexpected_file
            or still_registered_in_app_py
            or legacy_handlers_in_app_py
        ),
        "checked_route_count": len(expected_routes),
        "app_py_direct_routes": app_routes,
        "missing_in_blueprints": missing,
        "duplicated_in_blueprints": duplicated_in_blueprints,
        "registered_in_unexpected_file": registered_in_unexpected_file,
        "still_registered_in_app_py": still_registered_in_app_py,
        "legacy_split_handlers_in_app_py": legacy_handlers_in_app_py,
        "route_map_by_file": route_map_by_file,
        "groups": ROUTE_GROUPS,
        "group_by_route": {route: route_group_for(route) for route in sorted(expected_routes)},
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["ok"]:
        print("\nOK: split-route registration looks stable.")
        return 0
    print("\nFAIL: route split audit found issues.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
