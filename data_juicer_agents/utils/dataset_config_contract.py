# -*- coding: utf-8 -*-
"""Shared dataset-config contract metadata.

Single source of truth for:
- dataset root field classification used by DJConfigBridge
- source-mode contract exposed to LLM
- dataset source priority wording used by validation/guidance
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


DATASET_SOURCE_PRIORITY: Tuple[str, str, str] = (
    "generated_dataset_config",
    "dataset_path",
    "dataset",
)


DATASET_FIELDS: Tuple[str, ...] = (
    # Source selectors
    "dataset_path",
    "dataset",
    "generated_dataset_config",
    # Source runtime controls
    "validators",
    "load_dataset_kwargs",
    # Export settings
    "export_path",
    "export_type",
    "export_shard_size",
    "export_in_parallel",
    "export_extra_args",
    "export_aws_credentials",
    # Modality / binding keys
    "text_keys",
    "image_key",
    "image_bytes_key",
    "image_special_token",
    "audio_key",
    "audio_special_token",
    "video_key",
    "video_special_token",
    "eoc_special_token",
    # Dataset misc
    "suffixes",
    "keep_stats_in_res_ds",
    "keep_hashes_in_res_ds",
)


DATASET_NESTED_FIELDS: Dict[str, str] = {
    "dataset.configs[*]": "load strategy config object",
    "dataset.max_sample_num": "optional positive integer",
}


SOURCE_MODE_SPECS: Dict[str, Dict[str, Any]] = {
    "dataset_path": {
        "required_root_fields": ["dataset_path", "export_path"],
        "optional_root_fields": [
            "validators",
            "load_dataset_kwargs",
            "text_keys",
            "image_key",
            "audio_key",
            "video_key",
            "image_bytes_key",
        ],
        "forbidden_root_fields": ["generated_dataset_config", "dataset"],
        "notes": "Prefer this for single local/remote path inputs.",
    },
    "dataset": {
        "required_root_fields": ["dataset", "export_path"],
        "required_nested_fields": ["dataset.configs[*].type"],
        "optional_nested_fields": [
            "dataset.configs[*].source",
            "dataset.configs[*].path",
            "dataset.configs[*].weight",
            "dataset.max_sample_num",
        ],
        "optional_root_fields": [
            "validators",
            "load_dataset_kwargs",
            "text_keys",
            "image_key",
            "audio_key",
            "video_key",
            "image_bytes_key",
        ],
        "forbidden_root_fields": ["generated_dataset_config"],
        "notes": "Use this for advanced loading: mixture, source-specific configs, validators.",
    },
    "generated_dataset_config": {
        "required_root_fields": ["generated_dataset_config.type", "export_path"],
        "optional_root_fields": [
            "text_keys",
            "image_key",
            "audio_key",
            "video_key",
            "image_bytes_key",
        ],
        "forbidden_root_fields": ["dataset"],
        "notes": "Use formatter-based generated datasets; formatter params stay under generated_dataset_config.",
    },
}


def source_priority_text(*, sep: str = " > ") -> str:
    return sep.join(DATASET_SOURCE_PRIORITY)


def build_dataset_spec_contract() -> Dict[str, Any]:
    """Build a copy-safe LLM-facing spec contract payload."""
    source_modes: Dict[str, Dict[str, Any]] = {}
    for mode, spec in SOURCE_MODE_SPECS.items():
        entry: Dict[str, Any] = {}
        for key, value in spec.items():
            if isinstance(value, list):
                entry[key] = list(value)
            elif isinstance(value, dict):
                entry[key] = dict(value)
            else:
                entry[key] = value
        source_modes[mode] = entry

    return {
        "version": "v1",
        "placement": {
            "recipe_root_fields": list(DATASET_FIELDS),
            "dataset_nested_fields": dict(DATASET_NESTED_FIELDS),
        },
        "source_modes": source_modes,
        "conflict_policy": {
            "preferred_behavior": "Only provide one source mode in a recipe.",
            "runtime_priority_if_multiple_present": list(DATASET_SOURCE_PRIORITY),
        },
    }


__all__ = [
    "DATASET_FIELDS",
    "DATASET_NESTED_FIELDS",
    "DATASET_SOURCE_PRIORITY",
    "SOURCE_MODE_SPECS",
    "build_dataset_spec_contract",
    "source_priority_text",
]
