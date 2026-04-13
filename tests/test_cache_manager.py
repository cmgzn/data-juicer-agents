# -*- coding: utf-8 -*-
"""Unit tests for RetrievalCacheManager."""

import threading

import pytest

from data_juicer_agents.tools.retrieve._shared.backend.cache import (
    RetrievalCacheManager,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mgr():
    """Return a fresh RetrievalCacheManager for each test."""
    return RetrievalCacheManager()


# ---------------------------------------------------------------------------
# get / set
# ---------------------------------------------------------------------------


def test_get_returns_none_for_missing_key(mgr):
    assert mgr.get("nonexistent") is None


def test_set_and_get_roundtrip(mgr):
    mgr.set("key1", "value1")
    assert mgr.get("key1") == "value1"


def test_set_overwrites_existing_value(mgr):
    mgr.set("key1", "old")
    mgr.set("key1", "new")
    assert mgr.get("key1") == "new"


def test_set_stores_arbitrary_objects(mgr):
    obj = {"nested": [1, 2, 3]}
    mgr.set("obj", obj)
    assert mgr.get("obj") is obj


# ---------------------------------------------------------------------------
# invalidate
# ---------------------------------------------------------------------------


def test_invalidate_removes_value(mgr):
    mgr.set("key1", "value")
    mgr.invalidate("key1")
    assert mgr.get("key1") is None


def test_invalidate_nonexistent_key_is_noop(mgr):
    mgr.invalidate("ghost")  # should not raise


def test_invalidate_does_not_affect_other_keys(mgr):
    mgr.set("a", 1)
    mgr.set("b", 2)
    mgr.invalidate("a")
    assert mgr.get("a") is None
    assert mgr.get("b") == 2


# ---------------------------------------------------------------------------
# invalidate_all
# ---------------------------------------------------------------------------


def test_invalidate_all_clears_everything(mgr):
    mgr.set("a", 1)
    mgr.set("b", 2)
    mgr.invalidate_all()
    assert mgr.get("a") is None
    assert mgr.get("b") is None


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


def test_concurrent_set_and_get_are_thread_safe(mgr):
    """Multiple threads writing different keys should not corrupt state."""
    errors = []

    def writer(key, value):
        try:
            for _ in range(100):
                mgr.set(key, value)
                result = mgr.get(key)
                assert isinstance(result, str)
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=writer, args=(f"k{i}", f"v{i}")) for i in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread errors: {errors}"


def test_concurrent_invalidate_all_does_not_raise(mgr):
    """invalidate_all called concurrently with set should not raise."""
    errors = []

    def setter():
        try:
            for i in range(50):
                mgr.set(f"k{i}", i)
        except Exception as exc:
            errors.append(exc)

    def invalidator():
        try:
            for _ in range(50):
                mgr.invalidate_all()
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=setter),
        threading.Thread(target=invalidator),
        threading.Thread(target=setter),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread errors: {errors}"


# ---------------------------------------------------------------------------
# Module-level singleton smoke test
# ---------------------------------------------------------------------------


def test_module_singleton_is_accessible():
    """Verify the module-level cache_manager singleton is importable."""
    from data_juicer_agents.tools.retrieve._shared.backend.cache import cache_manager

    assert cache_manager is not None
