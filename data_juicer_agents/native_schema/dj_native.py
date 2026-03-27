# -*- coding: utf-8 -*-
"""Data-Juicer-backed native schema provider."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .descriptor import OperatorSchema, ParamDescriptor, ValidationIssue, ValidationResult


DATASET_SOURCE_PRIORITY: Tuple[str, str, str] = (
    "generated_dataset_config",
    "dataset_path",
    "dataset",
)

DATASET_FIELDS: Tuple[str, ...] = (
    "dataset_path",
    "dataset",
    "generated_dataset_config",
    "validators",
    "load_dataset_kwargs",
    "export_path",
    "export_type",
    "export_shard_size",
    "export_in_parallel",
    "export_extra_args",
    "export_aws_credentials",
    "text_keys",
    "image_key",
    "image_bytes_key",
    "image_special_token",
    "audio_key",
    "audio_special_token",
    "video_key",
    "video_special_token",
    "eoc_special_token",
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

SYSTEM_FIELD_HINTS: Tuple[str, ...] = (
    "adaptive_",
    "auto_",
    "backup_",
    "cache_",
    "checkpoint",
    "conflict_",
    "custom_operator_paths",
    "data_probe",
    "debug",
    "ds_cache_dir",
    "event_",
    "executor_type",
    "fusion_",
    "hpo_",
    "intermediate_storage",
    "max_log_size_mb",
    "max_partition_size_mb",
    "min_common_dep_num_to_combine",
    "np",
    "op_fusion",
    "op_list_",
    "open_",
    "partition",
    "percentiles",
    "preserve_intermediate_data",
    "ray_address",
    "resource_optimization",
    "save_stats_in_one_file",
    "skip_op_error",
    "temp_dir",
    "trace_",
    "turbo",
    "use_",
    "work_dir",
)

EXCLUDED_FIELDS: Set[str] = {"config", "help", "print_config", "process", "auto"}


def _type_name_from_value(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, tuple):
        names = sorted({_type_name_from_value(item) for item in value if item is not None})
        return " | ".join(names)
    if hasattr(value, "__name__"):
        return value.__name__
    return type(value).__name__


def _param_type_name(action: Any, default: Any) -> str:
    if getattr(action, "type", None) is not None:
        action_type = action.type
        if hasattr(action_type, "__name__"):
            return action_type.__name__
        return str(action_type)
    if getattr(action, "choices", None):
        choices = list(action.choices)
        if choices:
            return _type_name_from_value(choices[0])
    if isinstance(getattr(action, "const", None), bool):
        return "bool"
    return _type_name_from_value(default)


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


class DJNativeSchemaProvider:
    """Aggregates Data-Juicer schema and validation into a stable provider."""

    def __init__(self):
        self._parser = None
        self._global_schema: Optional[Dict[str, ParamDescriptor]] = None
        self._system_schema: Optional[Dict[str, ParamDescriptor]] = None
        self._dataset_schema: Optional[Dict[str, ParamDescriptor]] = None

    @property
    def parser(self):
        if self._parser is None:
            from data_juicer.config.config import build_base_parser

            self._parser = build_base_parser()
        return self._parser

    def _build_parser_with_ops(self, used_ops: Optional[Set[str]] = None):
        from data_juicer.config.config import (
            _collect_config_info_from_class_docs,
            build_base_parser,
            sort_op_by_types_and_names,
        )
        from data_juicer.ops.base_op import OPERATORS

        parser = build_base_parser()
        if used_ops:
            ops_sorted = sort_op_by_types_and_names(OPERATORS.modules.items())
            _collect_config_info_from_class_docs(
                [(name, cls) for name, cls in ops_sorted if name in used_ops],
                parser,
            )
        return parser

    def get_global_schema(self) -> Dict[str, ParamDescriptor]:
        if self._global_schema is not None:
            return self._global_schema

        schema: Dict[str, ParamDescriptor] = {}
        for action in self.parser._actions:
            if not hasattr(action, "dest"):
                continue
            dest = str(action.dest)
            if not dest or dest == "help" or dest in EXCLUDED_FIELDS:
                continue
            default = getattr(action, "default", None)
            choices = (
                list(action.choices)
                if getattr(action, "choices", None) is not None
                else []
            )
            required = bool(getattr(action, "required", False))
            native_group = self._resolve_native_group(dest)
            scope = "dataset" if native_group == "dataset" else "system" if native_group == "system" else "global"
            source_kind = "derived_native"
            descriptor = ParamDescriptor(
                path=dest,
                scope=scope,
                native_group=native_group,
                type_name=_param_type_name(action, default),
                required=required,
                default=default,
                choices=choices,
                description=str(getattr(action, "help", "") or "").strip(),
                source="parser",
                source_kind=source_kind,
                nested="." in dest,
                validation_modes=["parser"],
            )
            schema[dest] = descriptor

        self._global_schema = schema
        self._system_schema = {
            key: value for key, value in schema.items() if value.native_group == "system"
        }
        self._dataset_schema = {
            key: value for key, value in schema.items() if value.native_group == "dataset"
        }
        return schema

    def get_system_schema(self) -> Dict[str, ParamDescriptor]:
        if self._system_schema is None:
            self.get_global_schema()
        return dict(self._system_schema or {})

    def get_dataset_schema(self) -> Dict[str, ParamDescriptor]:
        if self._dataset_schema is None:
            self.get_global_schema()
        return dict(self._dataset_schema or {})

    def get_operator_schema(
        self, op_names: Optional[Set[str]] = None
    ) -> Dict[str, OperatorSchema]:
        try:
            from data_juicer.ops.base_op import OPERATORS

            known_op_names: Set[str] = set(OPERATORS.modules.keys())
        except Exception:
            known_op_names = set()

        if op_names is None:
            target_op_names = known_op_names
        else:
            target_op_names = known_op_names & set(op_names)
        if not target_op_names:
            return {}

        try:
            parser = self._build_parser_with_ops(target_op_names)
        except Exception:
            return {name: OperatorSchema(name=name) for name in target_op_names}

        schemas: Dict[str, OperatorSchema] = {
            name: OperatorSchema(name=name, params={}) for name in target_op_names
        }
        for action in parser._actions:
            dest = getattr(action, "dest", "")
            if not isinstance(dest, str) or "." not in dest:
                continue
            op_name, param_name = dest.split(".", 1)
            if op_name not in schemas:
                continue
            default = getattr(action, "default", None)
            descriptor = ParamDescriptor(
                path=dest,
                scope="operator",
                native_group="operator",
                type_name=_param_type_name(action, default),
                required=bool(getattr(action, "required", False)),
                default=default,
                choices=list(action.choices) if getattr(action, "choices", None) else [],
                description=str(getattr(action, "help", "") or "").strip(),
                source="op_registry",
                source_kind="derived_native",
                nested=True,
                validation_modes=["parser", "op_registry"],
            )
            schemas[op_name].params[param_name] = descriptor
        return schemas

    def classify_field(self, path: str) -> str:
        global_schema = self.get_global_schema()
        descriptor = global_schema.get(path)
        if descriptor is not None:
            return descriptor.native_group
        return self._resolve_native_group(path)

    def list_unclassified_fields(self) -> list[str]:
        return sorted(
            key
            for key, descriptor in self.get_global_schema().items()
            if descriptor.native_group == "misc"
        )

    def validate_system_config(self, config: Dict[str, Any]) -> ValidationResult:
        normalized, coercion_errors = self.coerce_config(config)
        parser_errors = self._validate_parser_payload(normalized, allowed_group="system")
        errors = list(parser_errors)
        warnings = [
            ValidationIssue(
                level="warning",
                path=key,
                code="type_coercion",
                message=message,
                source="parser",
            )
            for key, message in coercion_errors
        ]
        return ValidationResult(ok=not errors, errors=errors, warnings=warnings, normalized=normalized)

    def validate_dataset_config(self, config: Dict[str, Any]) -> ValidationResult:
        normalized = deepcopy(config)
        errors: List[ValidationIssue] = []
        warnings: List[ValidationIssue] = []

        source_count = int(bool(normalized.get("generated_dataset_config"))) + int(bool(normalized.get("dataset_path"))) + int(bool(normalized.get("dataset")))
        if source_count == 0:
            errors.append(self._issue("error", "dataset", "missing_required", "one of dataset_path, dataset, or generated_dataset_config is required", "derived_native"))
        elif source_count > 1:
            warnings.append(
                self._issue(
                    "warning",
                    "dataset",
                    "multiple_sources",
                    "multiple dataset sources are present; use one source mode only. If multiple are still provided, Data-Juicer priority is generated_dataset_config > dataset_path > dataset",
                    "derived_native",
                )
            )

        export_path = str(normalized.get("export_path", "") or "").strip()
        if not export_path:
            errors.append(self._issue("error", "export_path", "missing_required", "export_path is required", "derived_native"))
        elif not _is_probable_remote_export_path(export_path):
            export_parent = Path(export_path).expanduser().resolve().parent
            if not export_parent.exists():
                errors.append(
                    self._issue(
                        "error",
                        "export_path",
                        "missing_parent",
                        f"export parent directory does not exist: {export_parent}",
                        "dataset_builder",
                    )
                )

        dataset_path = str(normalized.get("dataset_path", "") or "").strip()
        if dataset_path:
            dataset_path_obj = Path(dataset_path).expanduser()
            if not dataset_path_obj.exists():
                if _is_probable_remote_dataset_path(dataset_path) or normalized.get("dataset") or normalized.get("generated_dataset_config"):
                    warnings.append(
                        self._issue(
                            "warning",
                            "dataset_path",
                            "remote_path_assumed",
                            f"dataset_path not found locally; treated as remote: {dataset_path}",
                            "derived_native",
                        )
                    )
                else:
                    errors.append(
                        self._issue(
                            "error",
                            "dataset_path",
                            "path_not_found",
                            f"dataset_path does not exist: {dataset_path}",
                            "dataset_builder",
                        )
                    )

        parser_payload = {}
        for key in self.get_dataset_schema().keys():
            if key in normalized and normalized[key] not in (None, "", [], {}):
                parser_payload[key] = normalized[key]
        errors.extend(self._validate_parser_payload(parser_payload, allowed_group="dataset"))

        dataset_cfg = normalized.get("dataset")
        if dataset_cfg is not None:
            ds_errors, ds_warnings = self._validate_dataset_object(dataset_cfg)
            errors.extend(ds_errors)
            warnings.extend(ds_warnings)

        generated_cfg = normalized.get("generated_dataset_config")
        if generated_cfg is not None:
            generated_errors = self._validate_generated_dataset_config(generated_cfg)
            errors.extend(generated_errors)

        return ValidationResult(ok=not errors, errors=errors, warnings=warnings, normalized=normalized)

    def validate_process_config(self, process: list[Dict[str, Any]]) -> ValidationResult:
        errors: List[ValidationIssue] = []
        warnings: List[ValidationIssue] = []
        if not process:
            errors.append(self._issue("error", "process", "missing_required", "operators must not be empty", "derived_native"))
            return ValidationResult(ok=False, errors=errors, warnings=warnings, normalized={"process": process})

        op_names: Set[str] = set()
        for index, item in enumerate(process):
            if not isinstance(item, dict) or len(item) != 1:
                errors.append(self._issue("error", f"process[{index}]", "invalid_shape", "each process entry must be an object with exactly one operator key", "derived_native"))
                continue
            op_name = next(iter(item.keys()))
            op_names.add(op_name)

        op_schemas = self.get_operator_schema(op_names)
        known_op_names = set(op_schemas.keys())
        for index, item in enumerate(process):
            if not isinstance(item, dict) or len(item) != 1:
                continue
            op_name, params = next(iter(item.items()))
            if op_name not in known_op_names:
                errors.append(self._issue("error", f"process[{index}].{op_name}", "unknown_operator", f"unknown operator '{op_name}'", "op_registry"))
                continue
            if not isinstance(params, dict):
                errors.append(self._issue("error", f"process[{index}].{op_name}", "invalid_params", "operator params must be an object", "derived_native"))
                continue
            valid_params = op_schemas[op_name].params
            for param_key in params.keys():
                if param_key not in valid_params:
                    errors.append(
                        self._issue(
                            "error",
                            f"process[{index}].{op_name}.{param_key}",
                            "unknown_param",
                            f"operators[{index}].{op_name}: unknown param '{param_key}'",
                            "op_registry",
                        )
                    )
        warnings.append(
            self._issue(
                "warning",
                "process",
                "deferred_runtime_validation",
                "operator parameter validation deferred; runtime errors will be used as the repair signal",
                "derived_native",
            )
        )
        return ValidationResult(ok=not errors, errors=errors, warnings=warnings, normalized={"process": deepcopy(process)})

    def validate_recipe(self, recipe: Dict[str, Any]) -> ValidationResult:
        errors: List[ValidationIssue] = []
        warnings: List[ValidationIssue] = []

        parser_errors = self._validate_parser_payload(recipe)
        errors.extend(parser_errors)

        dataset_payload = {key: value for key, value in recipe.items() if key in self.get_dataset_schema()}
        if dataset_payload:
            dataset_result = self.validate_dataset_config(dataset_payload)
            errors.extend(dataset_result.errors)
            warnings.extend(dataset_result.warnings)

        process_payload = recipe.get("process")
        if isinstance(process_payload, list) and process_payload:
            process_result = self.validate_process_config(process_payload)
            errors.extend(process_result.errors)
            warnings.extend(process_result.warnings)

        return ValidationResult(ok=not errors, errors=errors, warnings=warnings, normalized=deepcopy(recipe))

    def coerce_config(self, config: Dict[str, Any]) -> tuple[Dict[str, Any], list[Tuple[str, str]]]:
        if not config:
            return {}, []

        action_type_map: Dict[str, Any] = {}
        known_parser_dests: Set[str] = set()
        for action in self.parser._actions:
            dest = getattr(action, "dest", None)
            if not dest or dest == "help":
                continue
            known_parser_dests.add(dest)
            default = getattr(action, "default", None)
            action_type_map[dest] = type(default) if default is not None else None

        errors: list[Tuple[str, str]] = []
        coerced: Dict[str, Any] = {}
        bool_true = {"true", "1", "yes"}
        bool_false = {"false", "0", "no"}

        for key, value in config.items():
            expected_type = action_type_map.get(key)
            if expected_type is bool and isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in bool_true:
                    coerced[key] = True
                elif lowered in bool_false:
                    coerced[key] = False
                else:
                    coerced[key] = value
                    errors.append((key, f"Cannot coerce {key}={value!r} to bool; kept as-is."))
            elif expected_type is int and isinstance(value, str):
                try:
                    coerced[key] = int(value)
                except (TypeError, ValueError):
                    coerced[key] = value
                    errors.append((key, f"Cannot coerce {key}={value!r} to int; kept as-is."))
            elif expected_type is float and isinstance(value, str):
                try:
                    coerced[key] = float(value)
                except (TypeError, ValueError):
                    coerced[key] = value
                    errors.append((key, f"Cannot coerce {key}={value!r} to float; kept as-is."))
            else:
                coerced[key] = value
        return coerced, errors

    def build_dataset_contract(self) -> Dict[str, Any]:
        return {
            "version": "v1",
            "placement": {
                "recipe_root_fields": list(self.get_dataset_schema().keys()),
                "dataset_nested_fields": dict(DATASET_NESTED_FIELDS),
            },
            "source_modes": deepcopy(SOURCE_MODE_SPECS),
            "conflict_policy": {
                "preferred_behavior": "Only provide one source mode in a recipe.",
                "runtime_priority_if_multiple_present": list(DATASET_SOURCE_PRIORITY),
            },
        }

    def source_priority_text(self, *, sep: str = " > ") -> str:
        return sep.join(DATASET_SOURCE_PRIORITY)

    def _resolve_native_group(self, dest: str) -> str:
        if dest in DATASET_FIELDS:
            return "dataset"
        if dest == "process" or dest.startswith("process."):
            return "process"
        if "." in dest:
            return "system"
        if any(dest == hint or dest.startswith(hint) for hint in SYSTEM_FIELD_HINTS):
            return "system"
        return "misc"

    def _validate_parser_payload(
        self, config: Dict[str, Any], allowed_group: Optional[str] = None
    ) -> List[ValidationIssue]:
        if not config:
            return []
        try:
            from jsonargparse import Namespace

            filtered = {}
            for key, value in config.items():
                if key in EXCLUDED_FIELDS:
                    continue
                if allowed_group and self.classify_field(key) != allowed_group:
                    continue
                filtered[key] = value
            if not filtered:
                return []
            self.parser.validate(Namespace(**filtered))
            return []
        except Exception as exc:
            return [self._issue("error", allowed_group or "config", "parser_validation_failed", str(exc), "parser")]

    def _validate_dataset_object(
        self, dataset_cfg: Any
    ) -> Tuple[List[ValidationIssue], List[ValidationIssue]]:
        errors: List[ValidationIssue] = []
        warnings: List[ValidationIssue] = []
        if not isinstance(dataset_cfg, dict):
            errors.append(self._issue("error", "dataset", "invalid_shape", "dataset must be an object with a 'configs' list", "dataset_builder"))
            return errors, warnings

        configs = dataset_cfg.get("configs")
        if not isinstance(configs, list) or not configs:
            errors.append(self._issue("error", "dataset.configs", "missing_required", "dataset.configs must be a non-empty list", "dataset_builder"))
            return errors, warnings

        if "max_sample_num" in dataset_cfg:
            max_sample_num = dataset_cfg.get("max_sample_num")
            if not isinstance(max_sample_num, int) or max_sample_num <= 0:
                errors.append(self._issue("error", "dataset.max_sample_num", "invalid_value", "dataset.max_sample_num must be a positive integer", "dataset_builder"))

        normalized_types: List[str] = []
        for index, ds_config in enumerate(configs):
            if not isinstance(ds_config, dict):
                errors.append(self._issue("error", f"dataset.configs[{index}]", "invalid_shape", f"dataset.configs[{index}] must be an object", "dataset_builder"))
                continue
            data_type = str(ds_config.get("type", "")).strip()
            data_source = str(ds_config.get("source", "")).strip()
            if not data_type:
                errors.append(self._issue("error", f"dataset.configs[{index}].type", "missing_required", f"dataset.configs[{index}].type is required", "load_strategy"))
                continue
            normalized_types.append(data_type)
            try:
                from jsonargparse import Namespace
                from data_juicer.core.data.load_strategy import DataLoadStrategyRegistry

                strategy_cls = DataLoadStrategyRegistry.get_strategy_class(
                    "default", data_type, data_source or "*"
                )
                if strategy_cls is None:
                    errors.append(
                        self._issue(
                            "error",
                            f"dataset.configs[{index}]",
                            "missing_strategy",
                            f"dataset.configs[{index}] has no matching load strategy: type={data_type}, source={data_source or '*'}",
                            "load_strategy",
                        )
                    )
                else:
                    try:
                        strategy_cls(ds_config, cfg=Namespace())
                    except Exception as exc:
                        errors.append(
                            self._issue(
                                "error",
                                f"dataset.configs[{index}]",
                                "strategy_validation_failed",
                                f"dataset.configs[{index}] invalid: {exc}",
                                "load_strategy",
                            )
                        )
            except Exception as exc:
                warnings.append(
                    self._issue(
                        "warning",
                        f"dataset.configs[{index}]",
                        "strategy_validation_skipped",
                        f"dataset strategy validation skipped: {exc}",
                        "load_strategy",
                    )
                )

        normalized_type_set = {item for item in normalized_types if item}
        if len(normalized_type_set) > 1:
            errors.append(self._issue("error", "dataset.configs", "mixed_types", "mixture of different dataset source types is not supported", "dataset_builder"))
        if normalized_type_set == {"remote"} and len(configs) > 1:
            errors.append(self._issue("error", "dataset.configs", "multiple_remote", "multiple remote datasets are not supported", "dataset_builder"))

        return errors, warnings

    def _validate_generated_dataset_config(
        self, generated_cfg: Any
    ) -> List[ValidationIssue]:
        errors: List[ValidationIssue] = []
        if not isinstance(generated_cfg, dict):
            errors.append(self._issue("error", "generated_dataset_config", "invalid_shape", "generated_dataset_config must be an object", "formatter"))
            return errors
        obj_name = str(generated_cfg.get("type", "")).strip()
        if not obj_name:
            errors.append(self._issue("error", "generated_dataset_config.type", "missing_required", "generated_dataset_config.type is required", "formatter"))
            return errors
        try:
            from data_juicer.format.formatter import FORMATTERS

            if obj_name not in FORMATTERS.modules:
                errors.append(self._issue("error", "generated_dataset_config.type", "unknown_formatter", f"unknown generated dataset type '{obj_name}'", "formatter"))
        except Exception as exc:
            errors.append(self._issue("error", "generated_dataset_config.type", "formatter_registry_unavailable", f"formatter registry unavailable: {exc}", "formatter"))
        return errors

    def _issue(
        self, level: str, path: str, code: str, message: str, source: str
    ) -> ValidationIssue:
        return ValidationIssue(
            level=level, path=path, code=code, message=message, source=source
        )


_provider: Optional[DJNativeSchemaProvider] = None


def get_native_schema_provider() -> DJNativeSchemaProvider:
    global _provider
    if _provider is None:
        _provider = DJNativeSchemaProvider()
    return _provider


__all__ = [
    "DATASET_FIELDS",
    "DATASET_NESTED_FIELDS",
    "DATASET_SOURCE_PRIORITY",
    "DJNativeSchemaProvider",
    "SOURCE_MODE_SPECS",
    "get_native_schema_provider",
]
