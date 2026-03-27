# -*- coding: utf-8 -*-
"""Native schema aggregation interfaces."""

from .descriptor import OperatorSchema, ParamDescriptor, ValidationIssue, ValidationResult
from .dj_native import (
    DATASET_FIELDS,
    DATASET_NESTED_FIELDS,
    DATASET_SOURCE_PRIORITY,
    SOURCE_MODE_SPECS,
    DJNativeSchemaProvider,
    get_native_schema_provider,
)
from .provider import NativeSchemaProvider

__all__ = [
    "DATASET_FIELDS",
    "DATASET_NESTED_FIELDS",
    "DATASET_SOURCE_PRIORITY",
    "DJNativeSchemaProvider",
    "NativeSchemaProvider",
    "OperatorSchema",
    "ParamDescriptor",
    "SOURCE_MODE_SPECS",
    "ValidationIssue",
    "ValidationResult",
    "get_native_schema_provider",
]
