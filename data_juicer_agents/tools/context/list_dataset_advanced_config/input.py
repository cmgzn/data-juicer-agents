# -*- coding: utf-8 -*-
"""Input models for list_dataset_advanced_config."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ListDatasetAdvancedConfigInput(BaseModel):
    include_examples: bool = Field(
        default=True,
        description="Whether to include canonical configuration examples.",
    )
    include_descriptions: bool = Field(
        default=True,
        description="Whether to include docstring-style descriptions when available.",
    )


class GenericOutput(BaseModel):
    ok: bool = True
