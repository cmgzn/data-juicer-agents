# -*- coding: utf-8 -*-
"""Input models for retrieve_operators_api."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class RetrieveOperatorsAPIInput(BaseModel):
    intent: str = Field(
        description=(
            "Retrieval query for API-backed semantic retrieval. "
            "Provide a plain-text description of the desired operators."
        )
    )
    top_k: int = Field(default=10, ge=1, description="Maximum number of operator candidates to return.")
    mode: Literal["auto", "llm", "vector"] = Field(
        default="auto",
        description=(
            "API-backed retrieval mode. "
            "'auto': tries llm -> vector. "
            "'llm': semantic ranking via LLM. "
            "'vector': FAISS vector similarity."
        ),
    )
    op_type: str = Field(
        default="",
        description=(
            "Optional operator type filter (e.g. 'filter', 'mapper', 'deduplicator', "
            "'selector', 'grouper', 'aggregator', 'pipeline')."
        ),
    )
    tags: List[str] = Field(
        default_factory=list,
        description=(
            "Modality/resource tags to filter operators "
            "(e.g. 'text', 'image', 'multimodal', 'audio', 'video', 'cpu', 'gpu', 'api'). "
            "Only operators whose tag set contains ALL of the specified tags are returned."
        ),
    )
    dataset_path: str = Field(
        default="",
        description=(
            "Optional dataset file path for modality probing. When provided, the dataset "
            "modality is inferred and merged with any explicit tags. "
            "Mutually exclusive with 'dataset': provide at most one of dataset_path or dataset."
        ),
    )
    dataset: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional structured dataset config for modality probing: "
            '{"configs": [{"type": "local", "path": "..."}]}. '
            "Mutually exclusive with 'dataset_path': provide at most one of dataset or dataset_path."
        ),
    )


class GenericOutput(BaseModel):
    ok: bool = True
