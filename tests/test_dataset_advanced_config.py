# -*- coding: utf-8 -*-

from data_juicer_agents.tools.context import list_dataset_advanced_config
from data_juicer_agents.utils.dj_config_bridge import dataset_fields


def test_list_dataset_advanced_config_returns_unified_payload():
    payload = list_dataset_advanced_config(include_examples=True, include_descriptions=False)

    assert payload["ok"] is True
    assert payload["format_version"] == "v1"
    assert "spec_contract" in payload
    assert "dataset_builder_rules" in payload
    assert "dataset_fields" in payload
    assert "dataset_load_strategies" in payload
    assert "dataset_validators" in payload
    assert "generated_dataset_types" in payload
    assert "llm_guidance" in payload
    assert "examples" in payload

    rules = payload["dataset_builder_rules"]
    assert rules["source_priority"] == [
        "generated_dataset_config",
        "dataset_path",
        "dataset",
    ]
    assert "mixture_rules" in rules
    assert "legacy_dataset_path_rules" in rules

    contract = payload["spec_contract"]
    assert "source_modes" in contract
    assert "dataset_path" in contract["source_modes"]
    assert "dataset" in contract["source_modes"]
    assert "generated_dataset_config" in contract["source_modes"]
    assert set(contract["placement"]["recipe_root_fields"]) == set(dataset_fields)
