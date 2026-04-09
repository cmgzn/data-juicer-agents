# -*- coding: utf-8 -*-
"""Input models for inspect_dataset."""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

class InspectDatasetInput(BaseModel):
    dataset_path: str = Field(
        default="",
        description=(
            "Dataset file path to inspect. Will be converted to the standard "
            "dataset config format internally. Ignored when 'dataset' is provided."
        ),
    )
    dataset: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Structured multi-source dataset config in the standard format: "
            '{"configs": [{"type": "local", "path": "..."}]}. '
            "Takes priority over dataset_path when both are provided."
        ),
    )
    sample_size: int = Field(default=20, ge=1, description="Number of samples to inspect.")

class GenericOutput(BaseModel):
    ok: bool = True