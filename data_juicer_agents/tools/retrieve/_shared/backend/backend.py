# -*- coding: utf-8 -*-
"""Operator retrieval backend: data-source management and public API.

This module is now a thin coordination layer.  The heavy lifting has been
moved to dedicated modules:

* ``cache.py``         – thread-safe cache manager (replaces global variables)
* ``result_builder.py``– shared helpers for building result/trace dicts
* ``retriever.py``     – RetrieverBackend ABC, concrete backends,
                          and RetrievalStrategy

Public surface kept for backward-compatibility with existing callers and tests
that monkeypatch individual retrieval functions:
  retrieve_ops_lm_items, retrieve_ops_lm,
  retrieve_ops_bm25_items, retrieve_ops_bm25,
  retrieve_ops_regex_items, retrieve_ops_regex,
  retrieve_ops_with_meta, retrieve_ops
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from .cache import cache_manager
from .retriever import _strategy

# Internal cache key for the OPSearcher instance (managed here, not in cache.py)
_CK_OP_SEARCHER = "op_searcher"

# ---------------------------------------------------------------------------
# OPSearcher lifecycle
# ---------------------------------------------------------------------------

def refresh_op_catalog() -> bool:
    """Refresh the OPSearcher during agent runtime.

    Re-instantiates ``OPSearcher`` from the current
    ``OPERATORS.modules`` registry without reloading DJ modules.
    """
    try:
        logging.info("Refreshing op_searcher...")

        # Clear all caches to force rebuild
        cache_manager.invalidate_all()

        # Build a fresh OPSearcher via the bridge factory (applies patches)
        from data_juicer_agents.utils.dj_config_bridge import create_op_searcher

        new_searcher = create_op_searcher(include_formatter=False)
        cache_manager.set(_CK_OP_SEARCHER, new_searcher)
        logging.info(
            "Successfully refreshed op_searcher with %d operators",
            len(new_searcher.all_ops),
        )
        return True
    except Exception as e:
        import traceback

        traceback.print_exc()
        logging.error(f"Failed to refresh op_searcher: {e}")
        return False

def get_op_searcher():
    """Return the current OPSearcher instance (lifecycle-aware).

    After ``refresh_op_catalog()`` the cached searcher reflects the
    latest ``OPERATORS.modules`` state.  On first call (before any
    refresh) creates a default instance from the current registry.

    Uses double-checked locking to avoid creating multiple OPSearcher
    instances under concurrent access.
    """
    cached = cache_manager.get(_CK_OP_SEARCHER)
    if cached is not None:
        return cached

    # Double-checked locking: use cache_manager's internal lock to
    # ensure only one thread creates the default searcher.
    with cache_manager._lock:
        cached = cache_manager.get(_CK_OP_SEARCHER)
        if cached is not None:
            return cached

        # Lazy-init: create default searcher via the bridge factory (applies patches)
        from data_juicer_agents.utils.dj_config_bridge import create_op_searcher

        default = create_op_searcher(include_formatter=False)
        cache_manager.set(_CK_OP_SEARCHER, default)
        return default

# ---------------------------------------------------------------------------
# Thin wrappers – kept so existing tests can monkeypatch these names
# ---------------------------------------------------------------------------

async def retrieve_ops_lm_items(
    user_query: str,
    limit: int = 20,
    op_type: Optional[str] = None,
) -> List[dict]:
    """Thin wrapper: delegates to LLMRetriever.retrieve_items."""
    return await _strategy.backends["llm"].retrieve_items(user_query, limit=limit, op_type=op_type)

def retrieve_ops_bm25_items(
    user_query: str,
    limit: int = 20,
    op_type: Optional[str] = None,
) -> List[dict]:
    """Thin wrapper: BM25 retrieval – returns list of item dicts.

    Note: synchronous wrapper around the async backend for backward compat.
    """
    import asyncio

    async def _run():
        return await _strategy.backends["bm25"].retrieve_items(user_query, limit=limit, op_type=op_type)

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _run())
                return future.result()
        return loop.run_until_complete(_run())
    except RuntimeError:
        return asyncio.run(_run())

def retrieve_ops_regex_items(
    user_query: str,
    limit: int = 20,
    op_type: Optional[str] = None,
) -> List[dict]:
    """Thin wrapper: regex retrieval – returns list of item dicts.

    Note: synchronous wrapper around the async backend for backward compat.
    """
    import asyncio

    async def _run():
        return await _strategy.backends["regex"].retrieve_items(user_query, limit=limit, op_type=op_type)

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _run())
                return future.result()
        return loop.run_until_complete(_run())
    except RuntimeError:
        return asyncio.run(_run())

# ---------------------------------------------------------------------------
# Primary public API
# ---------------------------------------------------------------------------

async def retrieve_ops_with_meta(
    user_query: str,
    limit: int = 20,
    mode: str = "auto",
    op_type: Optional[str] = None,
    tags: Optional[list] = None,
) -> dict[str, Any]:
    """Tool retrieval with source/trace metadata.

    Delegates entirely to RetrievalStrategy.execute().

    Args:
        user_query: User query string.
        limit: Maximum number of tools to retrieve.
        mode: Retrieval mode – "llm", "bm25", "regex", or "auto".
        op_type: Optional operator type filter (e.g. "filter", "mapper").
        tags: List of tags to match.
    """
    return await _strategy.execute(user_query, limit=limit, mode=mode, op_type=op_type, tags=tags)

async def retrieve_ops(
    user_query: str,
    limit: int = 20,
    mode: str = "auto",
    op_type: Optional[str] = None,
) -> list:
    """Tool retrieval – returns list of tool names.

    Args:
        user_query: User query string.
        limit: Maximum number of tools to retrieve.
        mode: Retrieval mode – "llm", "bm25", "regex", or "auto".
        op_type: Optional operator type filter.
    """
    meta = await retrieve_ops_with_meta(
        user_query=user_query,
        limit=limit,
        mode=mode,
        op_type=op_type,
    )
    return list(meta.get("names", []))

