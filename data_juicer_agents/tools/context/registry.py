# -*- coding: utf-8 -*-
"""Registry for context tool specs."""

from __future__ import annotations

from typing import List

from data_juicer_agents.core.tool import ToolSpec

from .inspect_dataset.tool import INSPECT_DATASET
from .list_dataset_advanced_config.tool import LIST_DATASET_ADVANCED_CONFIG
from .list_system_config.tool import LIST_SYSTEM_CONFIG

TOOL_SPECS: List[ToolSpec] = [
    INSPECT_DATASET,
    LIST_SYSTEM_CONFIG,  # Discovery tool
    LIST_DATASET_ADVANCED_CONFIG,
]

__all__ = [
    "INSPECT_DATASET",
    "LIST_DATASET_ADVANCED_CONFIG",
    "LIST_SYSTEM_CONFIG",
    "TOOL_SPECS",
]
