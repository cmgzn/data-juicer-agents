# -*- coding: utf-8 -*-
"""Input models for list_dataset_load_strategies."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ListDatasetLoadStrategiesInput(BaseModel):
    """Input for listing Data-Juicer dataset load strategies.

    You can optionally filter by executor_type, data_type, or data_source.
    """

    executor_type: Optional[str] = Field(
        default=None,
        description="Optional executor_type filter (e.g., 'default', 'ray').",
    )
    data_type: Optional[str] = Field(
        default=None,
        description="Optional data_type filter (e.g., 'local', 'remote').",
    )
    data_source: Optional[str] = Field(
        default=None,
        description="Optional data_source filter (e.g., 'huggingface', 's3').",
    )
    include_descriptions: bool = Field(
        default=True,
        description="Whether to include strategy docstring descriptions.",
    )


class GenericOutput(BaseModel):
    ok: bool = True
