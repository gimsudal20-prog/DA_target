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
import re
import time
from typing import Any, Optional

import requests


DEFAULT_OPENAPI_BASE_URL = "https://api.searchad.naver.com"
_INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_openapi_credential(value: Any) -> str:
    """Normalize copied API credentials without changing meaningful key chars."""
    text = _INVISIBLE_CHARS_RE.sub("", str(value or ""))
    return _WHITESPACE_RE.sub("", text).strip()


def normalize_customer_id(value: Any) -> str:
    """Return a SearchAd Customer ID suitable for the X-Customer header."""
    text = _INVISIBLE_CHARS_RE.sub("", str(value or "")).strip()
    compact = _WHITESPACE_RE.sub("", text).replace(",", "")
    if re.fullmatch(r"\d+\.0+", compact):
        return compact.split(".", 1)[0]
    digits = re.sub(r"\D+", "", compact)
    return digits or compact


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
        normalize_openapi_credential(secret_key).encode("utf-8"),
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
    normalized_api_key = normalize_openapi_credential(api_key)
    normalized_secret_key = normalize_openapi_credential(secret_key)
    normalized_customer_id = normalize_customer_id(customer_id)
    return {
        "X-Timestamp": timestamp_ms,
        "X-API-KEY": normalized_api_key,
        "X-Customer": normalized_customer_id,
        "X-Signature": make_signature(timestamp_ms, method, uri, normalized_secret_key),
        "Content-Type": "application/json; charset=UTF-8",
    }


def _parse_error_payload(text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(text or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def format_naver_api_error(response: Any, operation: str = "네이버 API 요청") -> str:
    """Return a safe, user-readable error message without echoing signatures."""
    status_code = int(getattr(response, "status_code", 0) or 0)
    raw_text = str(getattr(response, "text", "") or "").strip()
    payload = {}
    try:
        parsed = response.json()
        payload = parsed if isinstance(parsed, dict) else {}
    except Exception:
        payload = _parse_error_payload(raw_text)

    problem_type = str(payload.get("type") or "").strip()
    title = str(payload.get("title") or payload.get("detail") or payload.get("message") or "").strip()
    combined = " ".join([problem_type, title, raw_text]).lower()

    if status_code == 403:
        if "signature" in payload or "invalid" in combined or "signature" in combined:
            return (
                f"{operation} 인증 실패(403): 네이버가 요청 서명을 거절했습니다. "
                "OPEN API KEY와 SECRET KEY가 같은 라이선스에서 복사된 값인지, 선택한 광고주 ID가 그 라이선스의 Customer ID인지, "
                "PC 시간이 현재 시간과 크게 어긋나지 않았는지 확인해주세요."
            )
        return (
            f"{operation} 권한 오류(403): 현재 API 라이선스가 선택한 광고주에 접근할 권한이 없습니다. "
            "광고주 ID와 API 라이선스 계정을 확인해주세요."
        )
    if status_code == 401:
        return f"{operation} 인증 실패(401): API KEY 또는 SECRET KEY를 다시 확인해주세요."
    if status_code:
        safe_parts = [x for x in [title, problem_type] if x]
        if safe_parts:
            return f"{operation} 실패({status_code}): {' | '.join(safe_parts)}"
        return f"{operation} 실패({status_code}): {raw_text or '네이버 API 오류'}"
    return f"{operation} 실패: {raw_text or '네이버 API 오류'}"


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
            # 일부 네트워크 예외는 str(exc)가 빈 문자열이라 프론트에
            # "알 수 없는 오류"만 노출된다. 예외 타입/원인까지 남겨 후속 조치가 가능하게 한다.
            parts: list[str] = [type(exc).__name__]
            msg = str(exc).strip()
            if msg:
                parts.append(msg)
            cause = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
            if cause:
                cause_msg = str(cause).strip() or repr(cause)
                parts.append(f"cause={type(cause).__name__}: {cause_msg}")
            last_err = " | ".join(dict.fromkeys([p for p in parts if p]))
            time.sleep(1.25)

    return make_fake_response(500, f"네트워크 통신 실패: {last_err or '알 수 없는 오류'}")
