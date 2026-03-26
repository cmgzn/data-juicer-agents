# -*- coding: utf-8 -*-

import asyncio


def test_retrieve_ops_with_meta_auto_uses_llm_when_available(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as retrieval_mod

    async def fake_llm_items(_query, limit=20):  # noqa: ARG001
        return [{"tool_name": "text_length_filter"}][:limit]

    monkeypatch.setenv("DASHSCOPE_API_KEY", "test-key")
    monkeypatch.setattr(retrieval_mod, "retrieve_ops_lm_items", fake_llm_items)
    monkeypatch.setattr(
        retrieval_mod,
        "retrieve_ops_vector",
        lambda _query, limit=20: ["document_deduplicator"][:limit],
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

    async def fail_llm_items(_query, limit=20):  # noqa: ARG001
        raise ImportError("cannot import async_sessionmaker")

    monkeypatch.setenv("DASHSCOPE_API_KEY", "test-key")
    monkeypatch.setattr(retrieval_mod, "retrieve_ops_lm_items", fail_llm_items)
    monkeypatch.setattr(
        retrieval_mod,
        "retrieve_ops_vector",
        lambda _query, limit=20: ["text_length_filter"][:limit],
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

    async def fail_llm_items(_query, limit=20):  # noqa: ARG001
        raise RuntimeError("llm unavailable")

    def fail_vector(_query, limit=20):  # noqa: ARG001
        raise RuntimeError("vector unavailable")

    def fail_bm25(_query, limit=20):  # noqa: ARG001
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


def test_retrieve_ops_with_meta_auto_without_api_key_falls_back_to_bm25(monkeypatch):
    from data_juicer_agents.tools.retrieve.retrieve_operators import backend as retrieval_mod

    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("MODELSCOPE_API_TOKEN", raising=False)
    monkeypatch.setattr(
        retrieval_mod,
        "retrieve_ops_bm25_items",
        lambda _query, limit=20: [
            {
                "tool_name": "document_deduplicator",
                "description": "deduplicate text records",
                "relevance_score": 100.0,
                "score_source": "bm25_rank",
                "operator_type": "deduplicator",
                "key_match": ["deduplicate", "text"],
            }
        ][:limit],
    )

    payload = asyncio.run(
        retrieval_mod.retrieve_ops_with_meta(
            "deduplicate text",
            limit=5,
            mode="auto",
        )
    )

    assert payload["names"] == ["document_deduplicator"]
    assert payload["source"] == "bm25"
    assert payload["trace"][0] == {
        "backend": "llm",
        "status": "skipped",
        "reason": "missing_api_key",
    }
    assert payload["trace"][1] == {
        "backend": "vector",
        "status": "skipped",
        "reason": "missing_api_key",
    }
    assert payload["trace"][2] == {"backend": "bm25", "status": "success"}
