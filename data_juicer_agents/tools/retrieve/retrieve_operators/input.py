# -*- coding: utf-8 -*-
"""Input models for retrieve_operators."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class RetrieveOperatorsInput(BaseModel):
    intent: str = Field(description="Natural-language retrieval intent.")
    top_k: int = Field(default=10, ge=1, description="Maximum number of operator candidates to return.")
    mode: Literal["auto", "llm", "vector", "bm25"] = Field(
        default="auto",
        description="Retrieval mode: auto, llm, vector, or bm25.",
    )
    dataset_path: str = Field(default="", description="Optional dataset path used as explicit retrieval context.")


class GenericOutput(BaseModel):
    ok: bool = True
