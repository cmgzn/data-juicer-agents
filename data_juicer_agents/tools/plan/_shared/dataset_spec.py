# -*- coding: utf-8 -*-
"""Shared dataset-spec helpers for plan tools."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .normalize import normalize_optional_text, normalize_params, normalize_string_list
from .schema import DatasetBindingSpec, DatasetSpec, _ALLOWED_MODALITIES


def normalize_dataset_spec(
    dataset_spec: DatasetSpec | Dict[str, Any],
) -> DatasetSpec:
    """Normalize dataset spec: strip strings, deduplicate lists."""
    if isinstance(dataset_spec, DatasetSpec):
        source = dataset_spec
    elif isinstance(dataset_spec, dict):
        source = DatasetSpec.from_dict(dataset_spec)
    else:
        raise ValueError("dataset_spec must be a dict object")

    return DatasetSpec.from_dict(
        {
            "io": {
                "dataset_path": str(source.io.dataset_path or "").strip(),
                "dataset": (
                    normalize_params(source.io.dataset)
                    if isinstance(source.io.dataset, dict)
                    else None
                ),
                "generated_dataset_config": (
                    normalize_params(source.io.generated_dataset_config)
                    if isinstance(source.io.generated_dataset_config, dict)
                    else None
                ),
                "export_path": str(source.io.export_path or "").strip(),
            },
            "binding": {
                "modality": str(source.binding.modality or "unknown").strip()
                or "unknown",
                "text_keys": normalize_string_list(source.binding.text_keys),
                "image_key": normalize_optional_text(source.binding.image_key),
                "audio_key": normalize_optional_text(source.binding.audio_key),
                "video_key": normalize_optional_text(source.binding.video_key),
                "image_bytes_key": normalize_optional_text(
                    source.binding.image_bytes_key
                ),
            },
            "warnings": normalize_string_list(source.warnings),
        }
    )


def infer_modality(binding: DatasetBindingSpec) -> str:
    candidate = str(binding.modality or "unknown").strip().lower()
    has_text = bool(binding.text_keys)
    has_image = bool(binding.image_key)
    has_audio = bool(binding.audio_key)
    has_video = bool(binding.video_key)

    if candidate in {"text", "image", "audio", "video", "multimodal"}:
        if candidate == "text" and has_text:
            return candidate
        if candidate == "image" and has_image:
            return candidate
        if candidate == "audio" and has_audio:
            return candidate
        if candidate == "video" and has_video:
            return candidate
        if candidate == "multimodal" and sum([has_text, has_image, has_audio, has_video]) >= 2:
            return candidate

    active_modalities = sum([has_text, has_image, has_audio, has_video])
    if active_modalities >= 2:
        return "multimodal"
    if has_video:
        return "video"
    if has_audio:
        return "audio"
    if has_image:
        return "image"
    if has_text:
        return "text"
    return "unknown"


def _is_probable_remote_dataset_path(path: str) -> bool:
    if not path:
        return False
    try:
        from data_juicer.utils.file_utils import is_absolute_path, is_remote_path
    except Exception:
        is_remote_path = lambda p: str(p).startswith(("http://", "https://", "s3://", "gs://", "hdfs://"))
        is_absolute_path = lambda p: os.path.isabs(str(p))

    if is_remote_path(path):
        return True
    # HuggingFace-style dataset id: "org/name" or "name"
    if not is_absolute_path(path) and not str(path).startswith(".") and str(path).count("/") <= 1:
        return True
    return False


def _is_probable_remote_export_path(path: str) -> bool:
    if not path:
        return False
    try:
        from data_juicer.utils.file_utils import is_remote_path
    except Exception:
        is_remote_path = lambda p: str(p).startswith(("http://", "https://", "s3://", "gs://", "hdfs://"))
    return bool(is_remote_path(path))


def _validate_dataset_config(dataset_cfg: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    if not isinstance(dataset_cfg, dict):
        errors.append("dataset must be an object with a 'configs' list")
        return errors, warnings

    configs = dataset_cfg.get("configs")
    if not isinstance(configs, list) or not configs:
        errors.append("dataset.configs must be a non-empty list")
        return errors, warnings

    if "max_sample_num" in dataset_cfg:
        max_sample_num = dataset_cfg.get("max_sample_num")
        if not isinstance(max_sample_num, int) or max_sample_num <= 0:
            errors.append("dataset.max_sample_num must be a positive integer")

    normalized_types: List[str] = []
    for idx, ds_config in enumerate(configs):
        if not isinstance(ds_config, dict):
            errors.append(f"dataset.configs[{idx}] must be an object")
            continue
        data_type = str(ds_config.get("type", "")).strip()
        data_source = str(ds_config.get("source", "")).strip()
        if not data_type:
            errors.append(f"dataset.configs[{idx}].type is required")
            continue
        normalized_types.append(data_type)

        try:
            from jsonargparse import Namespace
            from data_juicer.core.data.load_strategy import DataLoadStrategyRegistry

            strategy_cls = DataLoadStrategyRegistry.get_strategy_class(
                "default",
                data_type,
                data_source or "*",
            )
            if strategy_cls is None:
                errors.append(
                    f"dataset.configs[{idx}] has no matching load strategy: type={data_type}, source={data_source or '*'}"
                )
            else:
                try:
                    strategy_cls(ds_config, cfg=Namespace())
                except Exception as exc:
                    errors.append(f"dataset.configs[{idx}] invalid: {exc}")
        except Exception as exc:
            warnings.append(f"dataset strategy validation skipped: {exc}")

    normalized_type_set = {item for item in normalized_types if item}
    if len(normalized_type_set) > 1:
        errors.append("mixture of different dataset source types is not supported")
    if normalized_type_set == {"remote"} and len(configs) > 1:
        errors.append("multiple remote datasets are not supported")

    return errors, warnings


def _dataset_source_priority_warning(source_count: int) -> str | None:
    if source_count <= 1:
        return None
    return (
        "multiple dataset sources are present; current implementation is local-path-first and will "
        "follow Data-Juicer source priority generated_dataset_config > dataset_path > dataset"
    )


def validate_dataset_spec_payload(
    dataset_spec: DatasetSpec | Dict[str, Any],
    *,
    dataset_profile: Dict[str, Any] | None = None,
) -> Tuple[List[str], List[str]]:
    """Validate dataset spec with our business rules + DJ parser."""
    if isinstance(dataset_spec, dict):
        dataset_spec = DatasetSpec.from_dict(dataset_spec)

    errors: List[str] = []
    warnings: List[str] = list(dataset_spec.warnings)
    io = dataset_spec.io
    binding = dataset_spec.binding

    source_count = int(bool(io.generated_dataset_config)) + int(bool(io.dataset_path)) + int(bool(io.dataset))
    source_warning = _dataset_source_priority_warning(source_count)
    if source_warning and source_warning not in warnings:
        warnings.append(source_warning)

    if not source_count:
        errors.append("one of dataset_path, dataset, or generated_dataset_config is required")
    if not io.export_path:
        errors.append("export_path is required")

    if io.dataset_path:
        dataset_path = Path(io.dataset_path).expanduser()
        if not dataset_path.exists():
            if _is_probable_remote_dataset_path(io.dataset_path) or io.dataset or io.generated_dataset_config:
                warnings.append(f"dataset_path not found locally; treated as remote: {io.dataset_path}")
            else:
                errors.append(f"dataset_path does not exist: {io.dataset_path}")

    if io.export_path:
        if not _is_probable_remote_export_path(io.export_path):
            export_parent = Path(io.export_path).expanduser().resolve().parent
            if not export_parent.exists():
                errors.append(f"export parent directory does not exist: {export_parent}")

    if binding.modality not in _ALLOWED_MODALITIES:
        errors.append("modality must be one of text/image/audio/video/multimodal/unknown")
    if binding.modality == "text" and not binding.text_keys:
        errors.append("text modality requires text_keys")
    if binding.modality == "image" and not binding.image_key:
        errors.append("image modality requires image_key")
    if binding.modality == "audio" and not binding.audio_key:
        errors.append("audio modality requires audio_key")
    if binding.modality == "video" and not binding.video_key:
        errors.append("video modality requires video_key")
    if binding.modality == "multimodal":
        active = sum([bool(binding.text_keys), bool(binding.image_key), bool(binding.audio_key), bool(binding.video_key)])
        if active < 2:
            errors.append("multimodal modality requires at least two bound modalities")

    if isinstance(dataset_profile, dict) and dataset_profile.get("ok"):
        known_keys = set(dataset_profile.get("keys", [])) if isinstance(dataset_profile.get("keys", []), list) else set()
        for key in binding.text_keys:
            if known_keys and key not in known_keys:
                errors.append(f"text key not found in inspected dataset profile: {key}")
        for field_name, value in {
            "image_key": binding.image_key,
            "audio_key": binding.audio_key,
            "video_key": binding.video_key,
            "image_bytes_key": binding.image_bytes_key,
        }.items():
            if value and known_keys and value not in known_keys:
                errors.append(f"{field_name} not found in inspected dataset profile: {value}")

    if io.generated_dataset_config is not None:
        if not isinstance(io.generated_dataset_config, dict):
            errors.append("generated_dataset_config must be an object")
        elif not str(io.generated_dataset_config.get("type", "")).strip():
            errors.append("generated_dataset_config.type is required")

    if io.dataset is not None:
        dataset_errors, dataset_warnings = _validate_dataset_config(io.dataset)
        errors.extend(dataset_errors)
        warnings.extend([item for item in dataset_warnings if item not in warnings])

    # DJ parser validation for dataset fields
    try:
        from data_juicer_agents.utils.dj_config_bridge import get_dj_config_bridge

        bridge = get_dj_config_bridge()
        dataset_dict: Dict[str, Any] = {}
        if io.dataset_path:
            dataset_dict["dataset_path"] = io.dataset_path
        if io.export_path:
            dataset_dict["export_path"] = io.export_path
        if binding.text_keys:
            dataset_dict["text_keys"] = list(binding.text_keys)
        if binding.image_key:
            dataset_dict["image_key"] = binding.image_key
        if binding.audio_key:
            dataset_dict["audio_key"] = binding.audio_key
        if binding.video_key:
            dataset_dict["video_key"] = binding.video_key
        if binding.image_bytes_key:
            dataset_dict["image_bytes_key"] = binding.image_bytes_key
        if dataset_dict:
            is_valid, dj_errors = bridge.validate(dataset_dict)
            if not is_valid:
                errors.extend(dj_errors)
    except Exception:
        pass

    return errors, warnings


def validate_dataset_config_payload(dataset_cfg: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    return _validate_dataset_config(dataset_cfg)


def is_probable_remote_dataset_path(path: str) -> bool:
    return _is_probable_remote_dataset_path(path)


def is_probable_remote_export_path(path: str) -> bool:
    return _is_probable_remote_export_path(path)


__all__ = [
    "infer_modality",
    "is_probable_remote_dataset_path",
    "is_probable_remote_export_path",
    "normalize_dataset_spec",
    "validate_dataset_config_payload",
    "validate_dataset_spec_payload",
]