# -*- coding: utf-8 -*-
"""Input models for register_custom_operators."""

from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class RegisterCustomOperatorsInput(BaseModel):
    custom_operator_paths: List[str] = Field(
        description=(
            "List of directory paths or .py file paths containing custom operators "
            "registered via @OPERATORS.register_module. "
            "Example: ['./custom_ops', './my_operators/special_filter.py']. "
            "Operators are loaded into the DJ registry for the current process."
        ),
    )


class GenericOutput(BaseModel):
    ok: bool = True
