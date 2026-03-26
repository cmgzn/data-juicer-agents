# -*- coding: utf-8 -*-

import asyncio


def test_retrieve_ops_with_meta_auto_uses_llm_when_available(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as retrieval_mod

    async def fake_llm_items(_query, limit=20, op_type=None):  # noqa: ARG001
        return [{"tool_name": "text_length_filter"}][:limit]

    monkeypatch.setenv("DASHSCOPE_API_KEY", "test-key")
    monkeypatch.setattr(retrieval_mod, "retrieve_ops_lm_items", fake_llm_items)
    monkeypatch.setattr(
        retrieval_mod,
        "retrieve_ops_vector",
        lambda _query, limit=20, op_type=None: ["document_deduplicator"][:limit],
    )

    payload = asyncio.run(
        retrieval_mod.retrieve_ops_with_meta(
            "filter long text",
            limit=5,
            mode="auto",
        )
    )

    assert payload["names"] == ["text_length_filter"]
    assert payload["source"] == "llm"
    assert payload["trace"] == [{"backend": "llm", "status": "success"}]


def test_retrieve_ops_with_meta_auto_falls_back_to_vector(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as retrieval_mod

    async def fail_llm_items(_query, limit=20, op_type=None):  # noqa: ARG001
        raise ImportError("cannot import async_sessionmaker")

    monkeypatch.setenv("DASHSCOPE_API_KEY", "test-key")
    monkeypatch.setattr(retrieval_mod, "retrieve_ops_lm_items", fail_llm_items)
    monkeypatch.setattr(
        retrieval_mod,
        "retrieve_ops_vector",
        lambda _query, limit=20, op_type=None: ["text_length_filter"][:limit],
    )

    payload = asyncio.run(
        retrieval_mod.retrieve_ops_with_meta(
            "filter long text",
            limit=5,
            mode="auto",
        )
    )

    assert payload["names"] == ["text_length_filter"]
    assert payload["source"] == "vector"
    assert payload["trace"][0]["backend"] == "llm"
    assert payload["trace"][0]["status"] == "failed"
    assert "async_sessionmaker" in payload["trace"][0]["error"]
    assert payload["trace"][1] == {"backend": "vector", "status": "success"}


def test_retrieve_ops_with_meta_auto_returns_empty_when_all_backends_fail(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as retrieval_mod

    async def fail_llm_items(_query, limit=20, op_type=None):  # noqa: ARG001
        raise RuntimeError("llm unavailable")

    def fail_vector(_query, limit=20, op_type=None):  # noqa: ARG001
        raise RuntimeError("vector unavailable")

    def fail_bm25(_query, limit=20, op_type=None):  # noqa: ARG001
        raise RuntimeError("bm25 unavailable")

    monkeypatch.setenv("DASHSCOPE_API_KEY", "test-key")
    monkeypatch.setattr(retrieval_mod, "retrieve_ops_lm_items", fail_llm_items)
    monkeypatch.setattr(retrieval_mod, "retrieve_ops_vector", fail_vector)
    monkeypatch.setattr(retrieval_mod, "retrieve_ops_bm25_items", fail_bm25)

    payload = asyncio.run(
        retrieval_mod.retrieve_ops_with_meta(
            "filter long text",
            limit=5,
            mode="auto",
        )
    )

    assert payload["names"] == []
    assert payload["source"] == ""
    assert payload["trace"] == [
        {"backend": "llm", "status": "failed", "error": "llm unavailable"},
        {"backend": "vector", "status": "failed", "error": "vector unavailable"},
        {"backend": "bm25", "status": "failed", "error": "bm25 unavailable"},
    ]

def test_get_dj_func_info_caches_fallback_result(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as mod

    fake_info = [{"class_name": "op_a", "class_desc": "desc a"}]

    # Clear global state
    monkeypatch.setattr(mod, "_global_dj_func_info", None)

    # Make init_dj_func_info fail so get_dj_func_info takes the fallback path
    monkeypatch.setattr(mod, "init_dj_func_info", lambda: False)

    # Mock the fallback import — patch catalog.dj_func_info via the module
    import types
    fake_catalog = types.ModuleType("fake_catalog")
    fake_catalog.dj_func_info = fake_info

    import sys
    catalog_key = "data_juicer_agents.tools.retrieve.retrieve_operators.catalog"
    original = sys.modules.get(catalog_key)
    sys.modules[catalog_key] = fake_catalog
    try:
        # First call — should trigger fallback and cache
        result1 = mod.get_dj_func_info()
        assert result1 == fake_info

        # After fallback, _global_dj_func_info should be set
        assert mod._global_dj_func_info is fake_info

        # Second call — should return cached value without re-initializing
        result2 = mod.get_dj_func_info()
        assert result2 is result1
    finally:
        if original is not None:
            sys.modules[catalog_key] = original
        else:
            sys.modules.pop(catalog_key, None)
        # Restore global state
        mod._global_dj_func_info = None


def test_refresh_dj_func_info_reads_from_reloaded_module(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as mod

    old_info = [{"class_name": "old_op", "class_desc": "old"}]
    new_info = [{"class_name": "new_op", "class_desc": "new"}]

    # Set initial state
    mod._global_dj_func_info = old_info

    # Track what importlib.reload does — we mock the catalog module
    import types
    fake_catalog = types.ModuleType("fake_catalog")
    fake_catalog.dj_func_info = new_info
    fake_catalog.searcher = None  # not used in this test

    reload_called = {"ops": False, "catalog": False}

    def fake_reload(module):
        if hasattr(module, "dj_func_info"):
            reload_called["catalog"] = True
            # Simulate reload updating the module attribute
            module.dj_func_info = new_info
        else:
            reload_called["ops"] = True
        return module

    import importlib
    monkeypatch.setattr(importlib, "reload", fake_reload)

    # Patch the import of catalog module
    import sys
    catalog_key = "data_juicer_agents.tools.retrieve.retrieve_operators.catalog"
    original_catalog = sys.modules.get(catalog_key)
    sys.modules[catalog_key] = fake_catalog

    ops_key = "data_juicer.ops"
    fake_ops = types.ModuleType("fake_ops")
    original_ops = sys.modules.get(ops_key)
    sys.modules[ops_key] = fake_ops

    try:
        result = mod.refresh_dj_func_info()
        assert result is True
        assert mod._global_dj_func_info == new_info
        assert reload_called["catalog"] is True
    finally:
        if original_catalog is not None:
            sys.modules[catalog_key] = original_catalog
        else:
            sys.modules.pop(catalog_key, None)
        if original_ops is not None:
            sys.modules[ops_key] = original_ops
        else:
            sys.modules.pop(ops_key, None)
        mod._global_dj_func_info = None


def test_retrieve_ops_vector_skips_disk_when_memory_cached(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as mod

    fake_tools_info = [
        {"class_name": "text_length_filter", "class_desc": "Filter by length", "class_type": "filter"},
        {"class_name": "document_deduplicator", "class_desc": "Dedup docs", "class_type": "deduplicator"},
    ]

    # Create a fake vector store with similarity_search
    class FakeVectorStore:
        def similarity_search(self, query, k=10):
            class FakeDoc:
                def __init__(self, idx):
                    self.metadata = {"index": idx}
            return [FakeDoc(i) for i in range(min(k, len(fake_tools_info)))]

    # Pre-populate memory caches
    monkeypatch.setattr(mod, "_cached_vector_store", FakeVectorStore())
    monkeypatch.setattr(mod, "_cached_tools_info", fake_tools_info)

    # _load_cached_index should NOT be called
    load_called = {"value": False}

    def fail_load():
        load_called["value"] = True
        raise AssertionError("_load_cached_index should not be called when memory cache is populated")

    monkeypatch.setattr(mod, "_load_cached_index", fail_load)

    result = mod.retrieve_ops_vector("filter text", limit=5)
    assert load_called["value"] is False
    assert "text_length_filter" in result


def test_retrieve_ops_with_meta_passes_op_type_to_bm25(monkeypatch):
    """Verify op_type is correctly propagated to BM25 backend."""
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as mod


    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("MODELSCOPE_API_TOKEN", raising=False)

    payload = asyncio.run(
        mod.retrieve_ops_with_meta(
            "filter text",
            limit=5,
            mode="bm25",
            op_type="filter",
        )
    )
    assert "text_length_filter" in payload["names"]
    assert payload["source"] == "bm25"


def test_retrieve_ops_with_meta_passes_op_type_to_llm(monkeypatch):
    """Verify op_type is correctly propagated to LLM backend."""
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as mod

    captured = {}

    async def fake_llm_items(query, limit=20, op_type=None):
        captured["op_type"] = op_type
        return [
            {
                "tool_name": "text_length_filter",
                "description": "filter by length",
                "relevance_score": 95.0,
                "score_source": "llm",
                "operator_type": "filter",
                "key_match": ["filter"],
            }
        ][:limit]

    monkeypatch.setenv("DASHSCOPE_API_KEY", "test-key")
    monkeypatch.setattr(mod, "retrieve_ops_lm_items", fake_llm_items)

    payload = asyncio.run(
        mod.retrieve_ops_with_meta(
            "filter text",
            limit=5,
            mode="llm",
            op_type="filter",
        )
    )

    assert captured["op_type"] == "filter"
    assert payload["names"] == ["text_length_filter"]
    assert payload["source"] == "llm"


def test_retrieve_ops_without_api_key_falls_back_to_bm25(monkeypatch):
    """retrieve_ops (top-level entry) should return BM25 results when no API
    key is configured, skipping LLM and vector backends entirely."""
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as mod

    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("MODELSCOPE_API_TOKEN", raising=False)

    names = asyncio.run(
        mod.retrieve_ops("deduplicate document", limit=5, mode="auto", op_type="deduplicator")
    )

    assert "document_deduplicator" in names



