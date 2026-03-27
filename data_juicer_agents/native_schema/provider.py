# -*- coding: utf-8 -*-
"""Provider protocol for native schema aggregation."""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, Set

from .descriptor import OperatorSchema, ParamDescriptor, ValidationResult


class NativeSchemaProvider(Protocol):
    """Stable interface for aggregated native schema access."""

    def get_global_schema(self) -> Dict[str, ParamDescriptor]:
        ...

    def get_system_schema(self) -> Dict[str, ParamDescriptor]:
        ...

    def get_dataset_schema(self) -> Dict[str, ParamDescriptor]:
        ...

    def get_operator_schema(
        self, op_names: Optional[Set[str]] = None
    ) -> Dict[str, OperatorSchema]:
        ...

    def classify_field(self, path: str) -> str:
        ...

    def list_unclassified_fields(self) -> list[str]:
        ...

    def validate_system_config(self, config: Dict[str, Any]) -> ValidationResult:
        ...

    def validate_dataset_config(self, config: Dict[str, Any]) -> ValidationResult:
        ...

    def validate_process_config(self, process: list[Dict[str, Any]]) -> ValidationResult:
        ...

    def validate_recipe(self, recipe: Dict[str, Any]) -> ValidationResult:
        ...

    def coerce_config(self, config: Dict[str, Any]) -> tuple[Dict[str, Any], list[str]]:
        ...


__all__ = ["NativeSchemaProvider"]
