# -*- coding: utf-8 -*-
"""Thread-safe cache manager for retrieval backends."""

from __future__ import annotations

import threading
from typing import Any


class RetrievalCacheManager:
    """Thread-safe key-value cache for retrieval backends."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._store: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str) -> Any:
        """Return the cached value for *key*, or ``None`` if absent."""
        with self._lock:
            return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        """Store *value* under *key*."""
        with self._lock:
            self._store[key] = value

    def invalidate(self, key: str) -> None:
        """Remove a single cache entry."""
        with self._lock:
            self._store.pop(key, None)

    def invalidate_all(self) -> None:
        """Clear all cached values."""
        with self._lock:
            self._store.clear()


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

cache_manager = RetrievalCacheManager()
