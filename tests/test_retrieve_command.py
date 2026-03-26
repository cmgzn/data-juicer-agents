# -*- coding: utf-8 -*-

import json

from data_juicer_agents.cli import main


def test_retrieve_command_json_output(monkeypatch, capsys):
    from data_juicer_agents.commands import retrieve_cmd as retrieve_mod

    payload = {
        "ok": True,
        "intent": "deduplicate text",
        "top_k": 3,
        "mode": "auto",
        "retrieval_source": "lexical",
        "candidate_count": 1,
        "gap_detected": False,
        "candidates": [
            {
                "rank": 1,
                "operator_name": "document_deduplicator",
                "operator_type": "deduplicator",
                "description": "dedup",
                "relevance_score": 88.0,
                "arguments_preview": [],
            }
        ],
        "notes": [],
    }

    monkeypatch.setattr(
        retrieve_mod,
        "retrieve_operator_candidates",
        lambda **_kwargs: payload,
    )

    code = main(["retrieve", "deduplicate text", "--json"])
    assert code == 0

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["candidate_count"] == 1
    assert data["candidates"][0]["operator_name"] == "document_deduplicator"


def test_retrieve_command_top_k_validation():
    code = main(["retrieve", "dedup", "--top-k", "0"])
    assert code == 2


def test_retrieve_command_accepts_bm25_mode(monkeypatch, capsys):
    from data_juicer_agents.commands import retrieve_cmd as retrieve_mod

    seen = {}

    def _fake_retrieve(**kwargs):
        seen.update(kwargs)
        return {
            "ok": True,
            "intent": kwargs["intent"],
            "top_k": kwargs["top_k"],
            "mode": kwargs["mode"],
            "retrieval_source": "bm25",
            "candidate_count": 1,
            "gap_detected": False,
            "candidates": [
                {
                    "rank": 1,
                    "operator_name": "document_deduplicator",
                    "operator_type": "deduplicator",
                    "description": "dedup",
                    "relevance_score": 100.0,
                    "arguments_preview": [],
                }
            ],
            "notes": [],
        }

    monkeypatch.setattr(retrieve_mod, "retrieve_operator_candidates", _fake_retrieve)
    code = main(["retrieve", "dedup text", "--mode", "bm25", "--json"])
    assert code == 0
    assert seen["mode"] == "bm25"
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["retrieval_source"] == "bm25"
