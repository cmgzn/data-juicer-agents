# -*- coding: utf-8 -*-
"""Tool spec for list_dataset_advanced_config."""

from __future__ import annotations

from pydantic import BaseModel

from data_juicer_agents.core.tool import ToolContext, ToolResult, ToolSpec

from .input import ListDatasetAdvancedConfigInput
from .logic import list_dataset_advanced_config


class ListDatasetAdvancedConfigOutput(BaseModel):
    ok: bool = True
    message: str = ""
    spec_contract: dict = {}
    dataset_builder_rules: dict = {}
    dataset_fields: dict = {}
    dataset_load_strategies: dict = {}
    dataset_validators: dict = {}
    generated_dataset_types: dict = {}


def _list_dataset_advanced_config(
    _ctx: ToolContext, args: ListDatasetAdvancedConfigInput
) -> ToolResult:
    payload = list_dataset_advanced_config(
        include_examples=args.include_examples,
        include_descriptions=args.include_descriptions,
    )
    return ToolResult.success(
        summary=str(payload.get("message", "listed dataset advanced config")),
        data=payload,
    )


LIST_DATASET_ADVANCED_CONFIG = ToolSpec(
    name="list_dataset_advanced_config",
    description=(
        "Return unified advanced dataset configuration metadata for LLM planning in one payload: "
        "spec contract (field placement + source-mode boundaries), dataset_builder rules, dataset fields, "
        "load strategies, validators, generated dataset types, and examples."
    ),
    input_model=ListDatasetAdvancedConfigInput,
    output_model=ListDatasetAdvancedConfigOutput,
    executor=_list_dataset_advanced_config,
    tags=("context", "discovery", "dataset"),
    effects="read",
    confirmation="none",
)


__all__ = ["LIST_DATASET_ADVANCED_CONFIG"]
