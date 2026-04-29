# -*- coding: utf-8 -*-
"""Lookup/fetch helpers for Naver Search Ad entities.

This module keeps the high-traffic read paths and their short-lived cache out of
``app.py`` while preserving the existing response shapes used by the frontend.
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils.labels import CAMPAIGN_TYPE_COLORS, label_adgroup_type, label_campaign_type

RequestFunc = Callable[..., Any]
TargetObjectFunc = Callable[[str, str, str, str, str], Tuple[Any, Optional[Dict[str, Any]]]]


def stable_cache_key(api_key: str, secret_key: str, cid: str, scope: str) -> str:
    raw = f"{scope}::{str(api_key or '').strip()}::{str(secret_key or '').strip()}::{str(cid or '').strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class LookupCache:
    """Thread-safe, short-lived lookup cache for campaigns/adgroups/channels."""

    def __init__(self, ttl_seconds: float = 60.0) -> None:
        self.ttl_seconds = ttl_seconds
        self._lock = threading.RLock()
        self.campaigns: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self.adgroups: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self.channels: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}

    def get(self, store: Dict[str, Tuple[float, Any]], key: str, ttl: Optional[float] = None):
        now = time.time()
        max_age = self.ttl_seconds if ttl is None else ttl
        with self._lock:
            item = store.get(key)
            if not item:
                return None
            ts, value = item
            if now - ts > max_age:
                store.pop(key, None)
                return None
            return copy.deepcopy(value)

    def set(self, store: Dict[str, Tuple[float, Any]], key: str, value: Any) -> None:
        with self._lock:
            store[key] = (time.time(), copy.deepcopy(value))

    def invalidate_account(self, api_key: str, secret_key: str, cid: str) -> None:
        camp_key = stable_cache_key(api_key, secret_key, cid, "campaigns")
        ch_key = stable_cache_key(api_key, secret_key, cid, "channels")
        with self._lock:
            self.campaigns.pop(camp_key, None)
            self.channels.pop(ch_key, None)
            self.adgroups.clear()


def campaign_label(value: Any) -> str:
    return str(label_campaign_type(value, default=str(value or "-")))


def adgroup_label(value: Any) -> str:
    return str(label_adgroup_type(value, default=str(value or "-")))


def normalize_campaign_item(item: Dict[str, Any]) -> Dict[str, Any]:
    item = item if isinstance(item, dict) else {}
    campaign_tp = item.get("campaignTp") or ""
    return {
        "id": item.get("nccCampaignId") or item.get("id") or "",
        "name": item.get("name") or "",
        "campaignTp": campaign_tp,
        "label": campaign_label(campaign_tp),
        "badgeColor": CAMPAIGN_TYPE_COLORS.get(campaign_tp, "gray"),
        "raw": item,
    }


def normalize_adgroup_item(item: Dict[str, Any]) -> Dict[str, Any]:
    item = item if isinstance(item, dict) else {}
    adgroup_type = item.get("adgroupType") or ""
    return {
        "id": item.get("nccAdgroupId") or "",
        "name": item.get("name") or "",
        "adgroupType": adgroup_type,
        "label": adgroup_label(adgroup_type),
        "pcChannelId": item.get("pcChannelId") or "",
        "mobileChannelId": item.get("mobileChannelId") or "",
        "nccCampaignId": item.get("nccCampaignId") or "",
        "raw": item,
    }


def normalize_channel_item(item: Dict[str, Any]) -> Dict[str, Any]:
    item = item if isinstance(item, dict) else {}
    channel_id = item.get("nccBusinessChannelId") or item.get("bizChannelId") or item.get("nccChannelId") or item.get("channelId") or ""
    name = item.get("name") or item.get("channelContents") or item.get("siteUrl") or channel_id
    return {
        "id": channel_id,
        "name": name,
        "channelTp": item.get("channelTp") or item.get("bizChannelType") or item.get("channelType") or "",
        "siteUrl": item.get("siteUrl") or "",
        "raw": item,
    }


def pc_mobile_label(pc: Any, mobile: Any) -> str:
    if pc is True and mobile is False:
        return "PC"
    if pc is False and mobile is True:
        return "MOBILE"
    if pc is True and mobile is True:
        return "ALL"
    return "UNKNOWN"


def extract_pc_mobile_flags(detail_obj: Dict[str, Any] | None, pm_target_obj: Dict[str, Any] | None = None) -> Tuple[Optional[bool], Optional[bool]]:
    detail_obj = detail_obj or {}
    pc = detail_obj.get("pcDevice")
    mobile = detail_obj.get("mobileDevice")
    if isinstance(pc, bool) and isinstance(mobile, bool):
        return pc, mobile
    target = (pm_target_obj or {}).get("target") if isinstance(pm_target_obj, dict) else None
    if isinstance(target, dict):
        pc_t = target.get("pc")
        mobile_t = target.get("mobile")
        if isinstance(pc_t, bool) and isinstance(mobile_t, bool):
            return pc_t, mobile_t
    return None, None


class LookupService:
    """Fetch and cache lookup entities without changing app route contracts."""

    def __init__(self, request_func: RequestFunc, cache: Optional[LookupCache] = None) -> None:
        self.request_func = request_func
        self.cache = cache or LookupCache()

    def fetch_campaigns(self, api_key: str, secret_key: str, cid: str):
        res = self.request_func("GET", api_key, secret_key, cid, "/ncc/campaigns")
        if res.status_code != 200:
            return res, []
        rows = [normalize_campaign_item(x) for x in (res.json() or [])]
        rows.sort(key=lambda x: (x["label"], x["name"]))
        return res, rows

    def fetch_campaign_detail(self, api_key: str, secret_key: str, cid: str, campaign_id: str):
        campaign_id = str(campaign_id or "").strip()
        if not campaign_id:
            return None
        try:
            res = self.request_func("GET", api_key, secret_key, cid, f"/ncc/campaigns/{campaign_id}")
            if res.status_code == 200 and isinstance(res.json(), dict):
                return res.json()
        except Exception:
            pass
        try:
            _, rows = self.fetch_campaigns(api_key, secret_key, cid)
            for row in (rows or []):
                if str(row.get("id") or "").strip() == campaign_id:
                    raw = row.get("raw")
                    if isinstance(raw, dict):
                        return raw
                    return {"nccCampaignId": row.get("id"), "campaignTp": row.get("campaignTp"), "name": row.get("name")}
        except Exception:
            pass
        return None

    def fetch_adgroup_detail(self, api_key: str, secret_key: str, cid: str, adgroup_id: str):
        res = self.request_func("GET", api_key, secret_key, cid, f"/ncc/adgroups/{adgroup_id}")
        if res.status_code == 200 and isinstance(res.json(), dict):
            return res, res.json()
        return res, None

    def enrich_adgroup_media_row(
        self,
        api_key: str,
        secret_key: str,
        cid: str,
        row: Dict[str, Any],
        target_object_func: Optional[TargetObjectFunc] = None,
    ) -> Dict[str, Any]:
        row = copy.deepcopy(row)
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        adgroup_id = str(row.get("id") or raw.get("nccAdgroupId") or "").strip()
        if not adgroup_id:
            return row
        detail_obj: Dict[str, Any] | None = None
        pm_target_obj: Dict[str, Any] | None = None
        res_detail, detail_obj = self.fetch_adgroup_detail(api_key, secret_key, cid, adgroup_id)
        if res_detail.status_code == 200 and isinstance(detail_obj, dict):
            merged_raw = copy.deepcopy(raw)
            merged_raw.update(detail_obj)
            raw = merged_raw
        if target_object_func and (not isinstance(raw.get("pcDevice"), bool) or not isinstance(raw.get("mobileDevice"), bool)):
            try:
                res_target, pm_target_obj = target_object_func(api_key, secret_key, cid, adgroup_id, "PC_MOBILE_TARGET")
                if res_target.status_code != 200:
                    pm_target_obj = None
            except Exception:
                pm_target_obj = None
        pc, mobile = extract_pc_mobile_flags(detail_obj if isinstance(detail_obj, dict) else raw, pm_target_obj)
        if isinstance(pc, bool):
            raw["pcDevice"] = pc
        if isinstance(mobile, bool):
            raw["mobileDevice"] = mobile
        target_summary = raw.get("targetSummary") if isinstance(raw.get("targetSummary"), dict) else {}
        media_label = pc_mobile_label(raw.get("pcDevice"), raw.get("mobileDevice"))
        if media_label != "UNKNOWN":
            target_summary["pcMobile"] = media_label
            raw["targetSummary"] = target_summary
            row["mediaType"] = media_label
        row["pcDevice"] = raw.get("pcDevice")
        row["mobileDevice"] = raw.get("mobileDevice")
        row["raw"] = raw
        return row

    def fetch_adgroups(
        self,
        api_key: str,
        secret_key: str,
        cid: str,
        campaign_id: str,
        enrich_media: bool = True,
        target_object_func: Optional[TargetObjectFunc] = None,
    ):
        res = self.request_func("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": campaign_id})
        if res.status_code != 200:
            return res, []
        rows = [normalize_adgroup_item(x) for x in (res.json() or [])]
        if enrich_media and rows:
            max_workers = min(8, max(1, len(rows)))
            ordered: Dict[int, Dict[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                future_map = {
                    ex.submit(self.enrich_adgroup_media_row, api_key, secret_key, cid, row, target_object_func): idx
                    for idx, row in enumerate(rows)
                }
                for fut in as_completed(future_map):
                    idx = future_map[fut]
                    try:
                        ordered[idx] = fut.result()
                    except Exception:
                        ordered[idx] = rows[idx]
            rows = [ordered[i] for i in range(len(rows))]
        rows.sort(key=lambda x: x["name"])
        return res, rows

    def fetch_channels(self, api_key: str, secret_key: str, cid: str):
        res = self.request_func("GET", api_key, secret_key, cid, "/ncc/channels")
        if res.status_code != 200:
            return res, []
        rows = [normalize_channel_item(item) for item in (res.json() or [])]
        return res, rows

    def fetch_first_biz_channel_id(self, api_key: str, secret_key: str, cid: str) -> str:
        res, rows = self.fetch_channels(api_key, secret_key, cid)
        if res.status_code != 200:
            return ""
        for item in rows or []:
            cid_val = str(item.get("id") or "").strip()
            if cid_val:
                return cid_val
        return ""

    def get_campaigns_cached(self, api_key: str, secret_key: str, cid: str, force: bool = False):
        cache_key = stable_cache_key(api_key, secret_key, cid, "campaigns")
        if not force:
            cached = self.cache.get(self.cache.campaigns, cache_key)
            if cached is not None:
                return None, cached, True
        res, rows = self.fetch_campaigns(api_key, secret_key, cid)
        if res.status_code == 200:
            self.cache.set(self.cache.campaigns, cache_key, rows)
        return res, rows, False

    def get_adgroups_cached(
        self,
        api_key: str,
        secret_key: str,
        cid: str,
        campaign_id: str,
        force: bool = False,
        enrich_media: bool = False,
        target_object_func: Optional[TargetObjectFunc] = None,
    ):
        cache_key = stable_cache_key(api_key, secret_key, cid, f"adgroups::{campaign_id}::enrich::{1 if enrich_media else 0}")
        if not force:
            cached = self.cache.get(self.cache.adgroups, cache_key)
            if cached is not None:
                return None, cached, True
        res, rows = self.fetch_adgroups(api_key, secret_key, cid, campaign_id, enrich_media=enrich_media, target_object_func=target_object_func)
        if res.status_code == 200:
            self.cache.set(self.cache.adgroups, cache_key, rows)
        return res, rows, False

    def get_channels_cached(self, api_key: str, secret_key: str, cid: str, force: bool = False):
        cache_key = stable_cache_key(api_key, secret_key, cid, "channels")
        if not force:
            cached = self.cache.get(self.cache.channels, cache_key)
            if cached is not None:
                return None, cached, True
        res, rows = self.fetch_channels(api_key, secret_key, cid)
        if res.status_code == 200:
            self.cache.set(self.cache.channels, cache_key, rows)
        return res, rows, False
