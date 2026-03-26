# -*- coding: utf-8 -*-
"""Pure logic for list_dataset_load_strategies."""

from __future__ import annotations

from typing import Any, Dict, Optional
from data_juicer.core.data.load_strategy import DataLoadStrategyRegistry


def _type_name(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, tuple):
        return " | ".join(sorted({_type_name(item) for item in value if item is not None}))
    if hasattr(value, "__name__"):
        return value.__name__
    return str(value)


def list_dataset_load_strategies(
    *,
    executor_type: Optional[str] = None,
    data_type: Optional[str] = None,
    data_source: Optional[str] = None,
    include_descriptions: bool = True,
) -> Dict[str, Any]:
    """List dataset load strategies from Data-Juicer's registry.

    Returns a mapping keyed by "executor_type/data_type/data_source" with
    required/optional fields, field types, and description.
    """
    try:

        results: Dict[str, Any] = {}
        exec_filter = str(executor_type or "").strip()
        type_filter = str(data_type or "").strip()
        source_filter = str(data_source or "").strip()

        for strategy_key, strategy_class in DataLoadStrategyRegistry._strategies.items():
            if exec_filter and strategy_key.executor_type != exec_filter:
                continue
            if type_filter and strategy_key.data_type != type_filter:
                continue
            if source_filter and strategy_key.data_source != source_filter:
                continue

            identifier = f"{strategy_key.executor_type}/{strategy_key.data_type}/{strategy_key.data_source}"
            validation_rules = getattr(strategy_class, "CONFIG_VALIDATION_RULES", {}) or {}
            description = (strategy_class.__doc__ or "").strip()

            entry = {
                "executor_type": strategy_key.executor_type,
                "data_type": strategy_key.data_type,
                "data_source": strategy_key.data_source,
                "class_name": strategy_class.__name__,
                "required_fields": list(validation_rules.get("required_fields", [])),
                "optional_fields": list(validation_rules.get("optional_fields", [])),
                "field_types": {
                    key: _type_name(val) for key, val in validation_rules.get("field_types", {}).items()
                },
                "custom_validators": sorted(list(validation_rules.get("custom_validators", {}).keys())),
            }
            if include_descriptions:
                entry["description"] = description
            results[identifier] = entry

        return {
            "ok": True,
            "message": f"Listed {len(results)} dataset load strategies",
            "strategies": results,
            "total_count": len(results),
            "filters": {
                "executor_type": exec_filter or None,
                "data_type": type_filter or None,
                "data_source": source_filter or None,
            },
        }
    except Exception as exc:
        return {
            "ok": False,
            "message": f"Failed to list dataset load strategies: {exc}",
            "strategies": {},
            "total_count": 0,
            "filters": {
                "executor_type": str(executor_type or "").strip() or None,
                "data_type": str(data_type or "").strip() or None,
                "data_source": str(data_source or "").strip() or None,
            },
        }


__all__ = ["list_dataset_load_strategies"]
