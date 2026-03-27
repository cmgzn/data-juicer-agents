# -*- coding: utf-8 -*-
"""Native schema descriptors and validation payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


NativeGroup = Literal["system", "dataset", "process", "operator", "misc"]
FieldScope = Literal["global", "system", "dataset", "process", "operator"]
IssueLevel = Literal["error", "warning"]


@dataclass(frozen=True)
class ParamDescriptor:
    """Unified parameter descriptor for native schema consumers."""

    path: str
    scope: FieldScope
    native_group: NativeGroup
    type_name: str = "Any"
    required: bool = False
    default: Any = None
    choices: List[Any] = field(default_factory=list)
    description: str = ""
    source: str = "derived_native"
    source_kind: str = "derived_native"
    nested: bool = False
    validation_modes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "scope": self.scope,
            "native_group": self.native_group,
            "type": self.type_name,
            "required": self.required,
            "default": self.default,
            "choices": list(self.choices),
            "description": self.description,
            "source": self.source,
            "source_kind": self.source_kind,
            "nested": self.nested,
            "validation_modes": list(self.validation_modes),
        }


@dataclass(frozen=True)
class OperatorSchema:
    """Schema metadata for one operator."""

    name: str
    params: Dict[str, ParamDescriptor] = field(default_factory=dict)
    source: str = "op_registry"
    source_kind: str = "derived_native"


@dataclass(frozen=True)
class ValidationIssue:
    """One normalized validation issue."""

    level: IssueLevel
    path: str
    code: str
    message: str
    source: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "level": self.level,
            "path": self.path,
            "code": self.code,
            "message": self.message,
            "source": self.source,
        }


@dataclass(frozen=True)
class ValidationResult:
    """Normalized validation result."""

    ok: bool
    errors: List[ValidationIssue] = field(default_factory=list)
    warnings: List[ValidationIssue] = field(default_factory=list)
    normalized: Optional[Dict[str, Any]] = None

    def error_messages(self) -> List[str]:
        return [item.message for item in self.errors]

    def warning_messages(self) -> List[str]:
        return [item.message for item in self.warnings]


__all__ = [
    "FieldScope",
    "IssueLevel",
    "NativeGroup",
    "OperatorSchema",
    "ParamDescriptor",
    "ValidationIssue",
    "ValidationResult",
]
