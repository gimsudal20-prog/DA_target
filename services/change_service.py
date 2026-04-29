# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Callable, Dict


class ChangeService:
    """Thin service wrapper for mutation/change routes while business logic remains in app.py."""

    def __init__(self, handlers: Dict[str, Callable]):
        self.handlers = dict(handlers or {})

    def call(self, name: str):
        handler = self.handlers.get(name)
        if not callable(handler):
            raise KeyError(f"Route handler not registered: {name}")
        return handler()
