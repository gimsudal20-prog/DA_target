# -*- coding: utf-8 -*-
"""Gunicorn settings for long-running Naver Ads bulk operations.

Local Flask can finish large copy jobs, but deployed gunicorn defaults often kill
requests after 30 seconds and return an HTML 500 page. This keeps the request
open long enough for bulk copy/delete operations to finish and return JSON.
"""

import os

timeout = int(os.environ.get("WEB_TIMEOUT", "300"))
graceful_timeout = int(os.environ.get("WEB_GRACEFUL_TIMEOUT", "30"))
workers = int(os.environ.get("WEB_CONCURRENCY", "1"))
keepalive = int(os.environ.get("WEB_KEEPALIVE", "5"))
