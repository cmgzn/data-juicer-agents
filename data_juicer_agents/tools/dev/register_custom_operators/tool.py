# -*- coding: utf-8 -*-
"""Tool spec for register_custom_operators."""

from __future__ import annotations

from data_juicer_agents.core.tool import ToolContext, ToolResult, ToolSpec

from .input import GenericOutput, RegisterCustomOperatorsInput
from .logic import register_custom_operators


def _register_custom_operators(
    _ctx: ToolContext, args: RegisterCustomOperatorsInput
) -> ToolResult:
    result = register_custom_operators(paths=args.custom_operator_paths)
    if result.get("ok"):
        return ToolResult.success(
            summary=str(result.get("message", "custom operators registered")),
            data=result,
        )
    return ToolResult.failure(
        summary=str(result.get("message", "custom operator registration failed")),
        error_type=str(result.get("error_type", "registration_failed")),
        data=result,
    )


REGISTER_CUSTOM_OPERATORS = ToolSpec(
    name="register_custom_operators",
    description=(
        "Load custom Data-Juicer operators into the registry for the current session. "
        "Must be called BEFORE retrieve_operators or build_process_spec when using "
        "custom operators, so that they are discoverable and their parameters are known."
    ),
    input_model=RegisterCustomOperatorsInput,
    output_model=GenericOutput,
    executor=_register_custom_operators,
    tags=("dev", "operator"),
    effects="write",
    confirmation="none",
)

__all__ = ["REGISTER_CUSTOM_OPERATORS"]
