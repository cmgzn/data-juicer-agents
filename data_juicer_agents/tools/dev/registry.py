# -*- coding: utf-8 -*-
"""Registry for dev tool specs."""

from __future__ import annotations

from typing import List

from data_juicer_agents.core.tool import ToolSpec

from .develop_operator.tool import DEVELOP_OPERATOR
from .register_custom_operators.tool import REGISTER_CUSTOM_OPERATORS

TOOL_SPECS: List[ToolSpec] = [DEVELOP_OPERATOR, REGISTER_CUSTOM_OPERATORS]

__all__ = ["DEVELOP_OPERATOR", "REGISTER_CUSTOM_OPERATORS", "TOOL_SPECS"]
