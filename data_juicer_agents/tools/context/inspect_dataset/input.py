# -*- coding: utf-8 -*-
"""Input models for inspect_dataset."""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

class InspectDatasetInput(BaseModel):
    dataset_path: str = Field(
        default="",
        description=(
            "Dataset file path to inspect. Mutually exclusive with 'dataset': "
            "provide exactly one of dataset_path or dataset, not both."
        ),
    )
    dataset: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Structured multi-source dataset config in the standard format: "
            '{"configs": [{"type": "local", "path": "..."}]}. '
            "Mutually exclusive with 'dataset_path': "
            "provide exactly one of dataset or dataset_path, not both."
        ),
    )
    sample_size: int = Field(default=20, ge=1, description="Number of samples to inspect.")

class GenericOutput(BaseModel):
    ok: bool = True