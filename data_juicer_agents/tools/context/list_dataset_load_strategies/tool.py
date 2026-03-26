# -*- coding: utf-8 -*-
"""Tool spec for list_dataset_load_strategies."""

from __future__ import annotations

from pydantic import BaseModel

from data_juicer_agents.core.tool import ToolContext, ToolResult, ToolSpec

from .input import ListDatasetLoadStrategiesInput
from .logic import list_dataset_load_strategies


class ListDatasetLoadStrategiesOutput(BaseModel):
    ok: bool = True
    message: str = ""
    strategies: dict = {}
    total_count: int = 0
    filters: dict = {}


def _list_dataset_load_strategies(
    _ctx: ToolContext, args: ListDatasetLoadStrategiesInput
) -> ToolResult:
    result = list_dataset_load_strategies(
        executor_type=args.executor_type,
        data_type=args.data_type,
        data_source=args.data_source,
        include_descriptions=args.include_descriptions,
    )

    if result.get("ok"):
        return ToolResult.success(
            summary=str(result.get("message", "listed dataset load strategies")),
            data=result,
        )
    return ToolResult.failure(
        summary=str(result.get("message", "list_dataset_load_strategies failed")),
        error_type="dataset_load_strategy_list_failed",
        data=result,
    )


LIST_DATASET_LOAD_STRATEGIES = ToolSpec(
    name="list_dataset_load_strategies",
    description=(
        "List all Data-Juicer dataset load strategies, including required/optional fields and field types. "
        "Use this before build_dataset_spec to decide how to populate the dataset.configs entries."
    ),
    input_model=ListDatasetLoadStrategiesInput,
    output_model=ListDatasetLoadStrategiesOutput,
    executor=_list_dataset_load_strategies,
    tags=("context", "discovery", "dataset"),
    effects="read",
    confirmation="none",
)


__all__ = ["LIST_DATASET_LOAD_STRATEGIES"]
