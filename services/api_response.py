# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional


def api_error(message: str, details: Any = None, **extra: Any) -> Dict[str, Any]:
    """Build the app's existing JSON error shape without forcing success wraps."""
    payload: Dict[str, Any] = {"error": message}
    if details not in (None, ""):
        payload["details"] = details
    payload.update({k: v for k, v in extra.items() if v is not None})
    return payload


def api_ok(**payload: Any) -> Dict[str, Any]:
    """Build a small standard success payload for endpoints that already return objects."""
    out: Dict[str, Any] = {"ok": True}
    out.update(payload)
    return out


def file_payload(file_obj: Any, *, mimetype: str, download_name: str) -> Dict[str, Any]:
    return {"file": file_obj, "mimetype": mimetype, "download_name": download_name}
