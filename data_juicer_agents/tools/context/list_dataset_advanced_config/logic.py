# -*- coding: utf-8 -*-
"""Pure logic for list_dataset_advanced_config."""

from __future__ import annotations

import inspect
from typing import Any, Dict, List

from data_juicer_agents.native_schema import get_native_schema_provider
from data_juicer_agents.utils.dataset_config_contract import (
    DATASET_SOURCE_PRIORITY,
    build_dataset_spec_contract,
)


def _type_name(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, tuple):
        names = sorted({_type_name(item) for item in value if item is not None})
        return " | ".join(names)
    if hasattr(value, "__name__"):
        return value.__name__
    return str(value)


def _build_dataset_fields(include_descriptions: bool) -> Dict[str, Any]:
    try:
        provider = get_native_schema_provider()
        descriptors = provider.get_dataset_schema()
        fields: Dict[str, Any] = {}
        for name, descriptor in descriptors.items():
            entry: Dict[str, Any] = {
                "default": descriptor.default,
                "type": descriptor.type_name,
                "source": descriptor.source,
                "native_group": descriptor.native_group,
            }
            if include_descriptions:
                entry["description"] = descriptor.description
            fields[name] = entry
        return {
            "ok": True,
            "fields": fields,
            "count": len(fields),
        }
    except Exception as exc:
        return {
            "ok": False,
            "fields": {},
            "count": 0,
            "error": str(exc),
        }


def _build_validator_catalog(include_descriptions: bool) -> Dict[str, Any]:
    try:
        from data_juicer.core.data.data_validator import DataValidatorRegistry

        validators: Dict[str, Any] = {}
        for name, cls in DataValidatorRegistry._validators.items():
            signature = inspect.signature(cls.__init__)
            params: List[Dict[str, Any]] = []
            for param_name, param in signature.parameters.items():
                if param_name in {"self", "args", "kwargs"}:
                    continue
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                param_entry: Dict[str, Any] = {
                    "name": param_name,
                    "required": param.default is inspect.Signature.empty,
                    "default": None if param.default is inspect.Signature.empty else param.default,
                }
                if param.annotation is not inspect.Signature.empty:
                    param_entry["type"] = _type_name(param.annotation)
                params.append(param_entry)

            entry: Dict[str, Any] = {
                "class_name": cls.__name__,
                "init_params": params,
            }
            if include_descriptions:
                entry["description"] = str((cls.__doc__ or "").strip().split("\n\n")[0]).strip()
            validators[str(name)] = entry

        return {
            "ok": True,
            "validators": validators,
            "count": len(validators),
        }
    except Exception as exc:
        return {
            "ok": False,
            "validators": {},
            "count": 0,
            "error": str(exc),
        }


def _build_generated_dataset_catalog(include_descriptions: bool) -> Dict[str, Any]:
    try:
        from data_juicer.format.formatter import FORMATTERS

        generators: Dict[str, Any] = {}
        for name, cls in FORMATTERS.modules.items():
            signature = inspect.signature(cls.__init__)
            params: List[Dict[str, Any]] = []
            for param_name, param in signature.parameters.items():
                if param_name in {"self", "args", "kwargs"}:
                    continue
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                param_entry: Dict[str, Any] = {
                    "name": param_name,
                    "required": param.default is inspect.Signature.empty,
                    "default": None if param.default is inspect.Signature.empty else param.default,
                }
                if param.annotation is not inspect.Signature.empty:
                    param_entry["type"] = _type_name(param.annotation)
                params.append(param_entry)
            entry: Dict[str, Any] = {
                "class_name": cls.__name__,
                "init_params": params,
            }
            if include_descriptions:
                entry["description"] = str((cls.__doc__ or "").strip().split("\n\n")[0]).strip()
            generators[str(name)] = entry

        return {
            "ok": True,
            "generators": generators,
            "count": len(generators),
        }
    except Exception as exc:
        return {
            "ok": False,
            "generators": {},
            "count": 0,
            "error": str(exc),
        }


def _build_load_strategy_catalog(include_descriptions: bool) -> Dict[str, Any]:
    try:
        from data_juicer.core.data.load_strategy import DataLoadStrategyRegistry

        results: Dict[str, Any] = {}
        for strategy_key, strategy_class in DataLoadStrategyRegistry._strategies.items():
            identifier = (
                f"{strategy_key.executor_type}/{strategy_key.data_type}/{strategy_key.data_source}"
            )
            validation_rules = (
                getattr(strategy_class, "CONFIG_VALIDATION_RULES", {}) or {}
            )

            entry: Dict[str, Any] = {
                "executor_type": strategy_key.executor_type,
                "data_type": strategy_key.data_type,
                "data_source": strategy_key.data_source,
                "class_name": strategy_class.__name__,
                "required_fields": list(validation_rules.get("required_fields", [])),
                "optional_fields": list(validation_rules.get("optional_fields", [])),
                "field_types": {
                    key: _type_name(val)
                    for key, val in validation_rules.get("field_types", {}).items()
                },
                "custom_validators": sorted(
                    list(validation_rules.get("custom_validators", {}).keys())
                ),
            }
            if include_descriptions:
                entry["description"] = (strategy_class.__doc__ or "").strip()
            results[identifier] = entry

        return {
            "ok": True,
            "strategies": results,
            "count": len(results),
        }
    except Exception as exc:
        return {
            "ok": False,
            "strategies": {},
            "count": 0,
            "error": str(exc),
        }


def _dataset_builder_rules() -> Dict[str, Any]:
    return {
        "source_priority": list(DATASET_SOURCE_PRIORITY),
        "dataset_object_required_shape": {
            "dataset": {
                "configs": "non-empty list",
                "max_sample_num": "optional positive int",
            }
        },
        "mixture_rules": {
            "enabled_by": "dataset.configs with multiple items",
            "weight_field": "weight (default 1.0 per config)",
            "sampling_switch": "max_sample_num",
            "behavior": "sample counts are allocated by normalized weights",
            "constraints": [
                "all configs must share the same type",
                "multiple remote datasets are not supported",
            ],
        },
        "legacy_dataset_path_rules": {
            "format": "'<w1> path1 <w2> path2 ...' or 'path'",
            "parsing": "weights default to 1.0 if omitted",
            "rewrite_local": {
                "output": {"type": "local", "path": "<path>", "weight": "<float>"}
            },
            "rewrite_hf_like": {
                "match": "non-absolute, non-dot path with <= 1 slash",
                "output": {"type": "huggingface", "path": "<path>", "split": "train"},
                "note": "legacy CLI rewrite behavior from dataset_builder",
            },
        },
        "validation_runtime": {
            "registry": "DataLoadStrategyRegistry",
            "strategy_validation": "CONFIG_VALIDATION_RULES on each strategy",
            "validator_registry": "DataValidatorRegistry",
        },
    }


def _spec_contract() -> Dict[str, Any]:
    return get_native_schema_provider().build_dataset_contract()


def _examples() -> Dict[str, Any]:
    return {
        "dataset_local_mixture": {
            "dataset": {
                "max_sample_num": 10000,
                "configs": [
                    {"type": "local", "path": "./data_a.jsonl", "weight": 0.7},
                    {"type": "local", "path": "./data_b.parquet", "weight": 0.3},
                ],
            }
        },
        "dataset_remote_huggingface": {
            "dataset": {
                "configs": [
                    {
                        "type": "remote",
                        "source": "huggingface",
                        "path": "HuggingFaceFW/fineweb",
                        "name": "CC-MAIN-2024-10",
                        "split": "train",
                    }
                ]
            }
        },
        "dataset_remote_s3": {
            "dataset": {
                "configs": [
                    {
                        "type": "remote",
                        "source": "s3",
                        "path": "s3://bucket/path/data.parquet",
                        "aws_region": "us-east-1",
                    }
                ]
            }
        },
        "generated_dataset_config": {
            "generated_dataset_config": {
                "type": "<formatter_name>",
                "<formatter_specific_param>": "<value>",
            }
        },
    }


def list_dataset_advanced_config(
    *,
    include_examples: bool = True,
    include_descriptions: bool = True,
) -> Dict[str, Any]:
    """Return all advanced dataset-configuration metadata for LLM planning."""
    load_strategies_payload = _build_load_strategy_catalog(include_descriptions)
    dataset_fields_payload = _build_dataset_fields(include_descriptions)
    validators_payload = _build_validator_catalog(include_descriptions)
    generators_payload = _build_generated_dataset_catalog(include_descriptions)

    payload: Dict[str, Any] = {
        "ok": True,
        "message": "Listed advanced dataset configuration metadata",
        "format_version": "v1",
        "spec_contract": _spec_contract(),
        "dataset_builder_rules": _dataset_builder_rules(),
        "dataset_fields": dataset_fields_payload.get("fields", {}),
        "dataset_field_count": int(dataset_fields_payload.get("count", 0)),
        "dataset_load_strategies": load_strategies_payload.get("strategies", {}),
        "dataset_load_strategy_count": int(load_strategies_payload.get("count", 0)),
        "dataset_validators": validators_payload.get("validators", {}),
        "dataset_validator_count": int(validators_payload.get("count", 0)),
        "generated_dataset_types": generators_payload.get("generators", {}),
        "generated_dataset_type_count": int(generators_payload.get("count", 0)),
        "llm_guidance": {
            "recommended_order": [
                "select source mode: generated_dataset_config | dataset | dataset_path",
                "map source-mode fields to recipe_root_fields/dataset_nested_fields in spec_contract",
                "if dataset is used, choose strategy by type/source and fill required_fields first",
                "fill required_fields first, then optional_fields",
                "for mixture, keep same type across all dataset.configs",
                "bind modality keys (text_keys/image_key/audio_key/video_key)",
            ],
            "hard_constraints": [
                "dataset.configs must be non-empty",
                "mixture of different dataset types is not supported",
                "multiple remote datasets are not supported",
            ],
        },
    }

    if include_examples:
        payload["examples"] = _examples()

    warnings: List[str] = []
    if not load_strategies_payload.get("ok", False):
        warnings.append("dataset load strategies unavailable")
    if not dataset_fields_payload.get("ok", False):
        warnings.append("dataset fields unavailable")
    if not validators_payload.get("ok", False):
        warnings.append("dataset validators unavailable")
    if not generators_payload.get("ok", False):
        warnings.append("generated dataset types unavailable")

    if warnings:
        payload["warnings"] = warnings

    return payload


__all__ = ["list_dataset_advanced_config"]
