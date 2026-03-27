# -*- coding: utf-8 -*-
"""Pure logic for list_system_config."""

from __future__ import annotations

from typing import Any, Dict, Optional

from data_juicer_agents.native_schema import get_native_schema_provider

def list_system_config(
    *,
    filter_prefix: Optional[str] = None,
    include_descriptions: bool = True
) -> Dict[str, Any]:
    """List system configuration from Data-Juicer.
    
    This function lists all available system configuration parameters
    from Data-Juicer, including their types, default values, and descriptions.
    
    Args:
        filter_prefix: Optional filter to show only parameters matching this prefix
        include_descriptions: Whether to include parameter descriptions
        
    Returns:
        Dict containing configuration information and available parameters
    """
    try:
        provider = get_native_schema_provider()
        config = {}
        for param_name, descriptor in provider.get_system_schema().items():
            if filter_prefix and not param_name.startswith(filter_prefix):
                continue
            entry = {
                "default": descriptor.default,
                "type": descriptor.type_name,
                "source": descriptor.source,
                "native_group": descriptor.native_group,
            }
            if include_descriptions:
                entry["description"] = descriptor.description
            config[param_name] = entry

        warnings = []
        unclassified_fields = provider.list_unclassified_fields()
        if unclassified_fields:
            warnings.append(
                f"The following {len(unclassified_fields)} DJ config field(s) are not yet "
                f"classified into system/dataset/process categories and have been "
                f"excluded from the listing: {unclassified_fields}"
            )

        result = {
            "ok": True,
            "message": f"Listed {len(config)} system configuration parameters",
            "config": config,
            "total_count": len(config),
            "filter_applied": filter_prefix,
        }
        if warnings:
            result["warnings"] = warnings
        return result
    except Exception as e:
        return {
            "ok": False,
            "message": f"Failed to list system config: {str(e)}",
            "config": {},
            "total_count": 0,
            "filter_applied": filter_prefix,
        }

__all__ = ["list_system_config"]
