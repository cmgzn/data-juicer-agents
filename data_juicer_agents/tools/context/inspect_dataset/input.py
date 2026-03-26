# -*- coding: utf-8 -*-
"""Input models for inspect_dataset."""

from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field, model_validator


class InspectDatasetInput(BaseModel):
    dataset_path: str = Field(default="", description="Dataset file path to inspect.")
    dataset: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional dataset config dict (Data-Juicer dataset.configs schema).",
    )
    generated_dataset_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional dataset generator config (Data-Juicer generated_dataset_config).",
    )
    sample_size: int = Field(default=20, ge=1, description="Number of samples to inspect.")

    @model_validator(mode="after")
    def _require_dataset_source(self):
        if not (self.dataset_path.strip() or self.dataset or self.generated_dataset_config):
            raise ValueError("dataset_path, dataset, or generated_dataset_config is required")
        return self


class GenericOutput(BaseModel):
    ok: bool = True
