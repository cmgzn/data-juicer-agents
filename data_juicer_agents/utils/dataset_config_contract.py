# -*- coding: utf-8 -*-
"""Compatibility wrappers for dataset config contract metadata."""

from __future__ import annotations

from typing import Any, Dict

from data_juicer_agents.native_schema import (
    DATASET_FIELDS,
    DATASET_NESTED_FIELDS,
    DATASET_SOURCE_PRIORITY,
    SOURCE_MODE_SPECS,
    get_native_schema_provider,
)


def source_priority_text(*, sep: str = " > ") -> str:
    return get_native_schema_provider().source_priority_text(sep=sep)


def build_dataset_spec_contract() -> Dict[str, Any]:
    """Build a copy-safe LLM-facing spec contract payload."""
    return get_native_schema_provider().build_dataset_contract()


__all__ = [
    "DATASET_FIELDS",
    "DATASET_NESTED_FIELDS",
    "DATASET_SOURCE_PRIORITY",
    "SOURCE_MODE_SPECS",
    "build_dataset_spec_contract",
    "source_priority_text",
]
