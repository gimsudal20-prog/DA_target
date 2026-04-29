# -*- coding: utf-8 -*-
"""Small, dependency-light client helpers for Naver SearchAd API calls.

This module intentionally keeps the public behavior used by app.py unchanged.
The first refactor step only centralizes repeated request/signature/session code;
routes can keep calling app.py's _do_req wrapper until route modules are split.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Optional

import requests


DEFAULT_OPENAPI_BASE_URL = "https://api.searchad.naver.com"


def create_naver_session(pool_connections: int = 32, pool_maxsize: int = 64) -> requests.Session:
    """Create a pooled requests session for SearchAd API traffic."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=0,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def make_signature(timestamp_ms: str, method: str, uri: str, secret_key: str) -> str:
    """Build the X-Signature value required by Naver SearchAd."""
    msg = f"{timestamp_ms}.{str(method).upper()}.{uri}"
    digest = hmac.new(
        str(secret_key).strip().encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode()


def build_open_headers(
    api_key: str,
    secret_key: str,
    customer_id: str,
    method: str,
    uri: str,
) -> dict[str, str]:
    """Build standard SearchAd OpenAPI headers."""
    timestamp_ms = str(int(time.time() * 1000))
    return {
        "X-Timestamp": timestamp_ms,
        "X-API-KEY": str(api_key).strip(),
        "X-Customer": str(customer_id).strip(),
        "X-Signature": make_signature(timestamp_ms, method, uri, secret_key),
        "Content-Type": "application/json; charset=UTF-8",
    }


class FakeResponse:
    """Minimal response-compatible object used when network retries all fail."""

    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8", errors="ignore")

    def json(self) -> dict[str, Any]:
        try:
            parsed = json.loads(self.text)
            return parsed if isinstance(parsed, dict) else {"data": parsed}
        except Exception:
            return {"error": self.text}


def make_fake_response(status_code: int, text: str) -> FakeResponse:
    """Return a response-like object for app.py's existing error handling."""
    return FakeResponse(status_code, text)


def request_naver_api(
    method: str,
    api_key: str,
    secret_key: str,
    customer_id: str,
    uri: str,
    *,
    params: Optional[dict[str, Any]] = None,
    json_body: Optional[dict[str, Any]] = None,
    max_retries: int = 3,
    session: Optional[requests.Session] = None,
    base_url: str = DEFAULT_OPENAPI_BASE_URL,
    timeout: tuple[int, int] = (5, 20),
) -> requests.Response | FakeResponse:
    """Execute a SearchAd API request with the existing retry semantics.

    Behavior intentionally matches the previous app.py _do_req helper:
    - return immediately on 200/201/204
    - retry 429 and 404/1018 briefly
    - return non-success API responses as-is
    - return a 500-like fake response after network failure retries
    """
    http = session or create_naver_session()
    url = str(base_url).rstrip("/") + uri
    last_err: Optional[str] = None

    for _ in range(max_retries):
        headers = build_open_headers(api_key, secret_key, customer_id, method, uri)
        try:
            response = http.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
            if response.status_code in {200, 201, 204}:
                return response
            if response.status_code == 429:
                time.sleep(1.25)
                continue
            if response.status_code == 404 and "1018" in response.text:
                time.sleep(1.0)
                continue
            return response
        except requests.exceptions.RequestException as exc:
            last_err = str(exc)
            time.sleep(1.25)

    return make_fake_response(500, f"네트워크 통신 실패: {last_err or '알 수 없는 오류'}")
