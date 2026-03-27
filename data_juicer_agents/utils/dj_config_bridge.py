# -*- coding: utf-8 -*-
"""Compatibility bridge built on top of NativeSchemaProvider."""

from typing import Any, Dict, List, Optional, Tuple

from data_juicer_agents.native_schema import get_native_schema_provider

# ---------------------------------------------------------------------------
# Field classification
# ---------------------------------------------------------------------------

# Fields automatically managed by the agent layer (not exposed to LLM).
# These are set programmatically during apply (e.g. project_name ← plan_id).
agent_managed_fields = [
    "project_name",
    "job_id",
    "auto",  # This is for auto-analyze mode, temporarily added here to avoid LLM exposure until we decide how to handle it.
    "config",  # This is for passing the full config dict to the agent for internal use, not for LLM configuration.
]

dataset_fields = list(get_native_schema_provider().get_dataset_schema().keys())
system_fields = list(get_native_schema_provider().get_system_schema().keys())

# ---------------------------------------------------------------------------
# Bridge class
# ---------------------------------------------------------------------------


class DJConfigBridge:
    """Bridge to Data-Juicer's native configuration and validation.

    All DJ-dependent logic is centralised here.  Callers should obtain
    the singleton via ``get_dj_config_bridge()`` and call methods on it.
    """

    def __init__(self):
        self._provider = get_native_schema_provider()
        self._default_config = None

    # -- parser helpers -----------------------------------------------------

    @property
    def parser(self):
        return self._provider.parser

    def _build_parser_with_ops(self, used_ops: Optional[set] = None):
        return self._provider._build_parser_with_ops(used_ops)

    # -- config extraction --------------------------------------------------

    def get_default_config(self) -> Dict[str, Any]:
        if self._default_config is None:
            defaults = {
                key: descriptor.default
                for key, descriptor in self._provider.get_global_schema().items()
            }
            defaults["process"] = []
            self._default_config = defaults
        return self._default_config

    def extract_system_config(
        self, config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Extract system-related fields based on the explicit ``system_fields`` list."""
        config_dict = config if config is not None else self.get_default_config()
        return {f: config_dict[f] for f in system_fields if f in config_dict}

    def extract_dataset_config(
        self, config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Extract dataset-related fields."""
        config_dict = config if config is not None else self.get_default_config()
        return {f: config_dict[f] for f in dataset_fields if f in config_dict}

    def extract_agent_managed_config(
        self, config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Extract agent-managed fields (auto-set by agent, not by LLM).

        These fields (e.g. ``project_name``) are programmatically set
        during the apply phase and should not be exposed to the LLM for
        configuration.
        """
        config_dict = config if config is not None else self.get_default_config()
        return {f: config_dict[f] for f in agent_managed_fields if f in config_dict}

    def extract_process_config(
        self, config: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Extract process operator list."""
        config_dict = config if config is not None else self.get_default_config()
        return config_dict.get("process", [])

    def get_param_descriptions(self) -> Dict[str, str]:
        """Get help text for all parameters from parser."""
        return {
            key: descriptor.description
            for key, descriptor in self._provider.get_global_schema().items()
        }

    # -- validation ---------------------------------------------------------

    def validate(self, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a config dict using DJ base parser.

        Checks system/dataset field types and rejects unknown keys.
        Does NOT validate process list contents or operator params
        (that is handled by get_op_valid_params in the agents layer).

        Args:
            config: Config dict to validate.

        Returns:
            ``(is_valid, error_messages)``
        """
        result = self._provider.validate_recipe(config)
        return result.ok, result.error_messages()

    # -- operator introspection ---------------------------------------------

    def get_op_valid_params(self, op_names: set) -> Tuple[Dict[str, set], set]:
        """Get valid parameter names for each operator.

        Registers the requested operators into a fresh parser, then
        extracts valid parameter names from the resulting flat actions
        (e.g. ``text_length_filter.min_len`` -> ``min_len``).

        Args:
            op_names: Set of operator names to look up.

        Returns:
            ``(op_param_map, known_op_names)`` where
            *op_param_map* is ``{op_name: {param, ...}}`` and
            *known_op_names* is the full set of registered DJ operators.
        """
        schemas = self._provider.get_operator_schema(op_names)
        known_op_names = set(self._provider.get_operator_schema().keys())
        return (
            {name: set(schema.params.keys()) for name, schema in schemas.items()},
            known_op_names,
        )


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_bridge = None


def get_dj_config_bridge() -> DJConfigBridge:
    """Get singleton DJConfigBridge instance."""
    global _bridge
    if _bridge is None:
        _bridge = DJConfigBridge()
    return _bridge


# ---------------------------------------------------------------------------
# Standalone utility (used by normalize layer, not a bridge wrapper)
# ---------------------------------------------------------------------------


def coerce_fields(fields: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Coerce field values to their correct basic Python types via DJ parser.

    Performs safe conversions for basic types (``bool``, ``int``, ``float``)
    by inspecting the DJ parser's registered default-value types.  Fields
    with non-basic target types or fields not registered in the parser are
    passed through unchanged.

    This is used during normalization to ensure values serialise correctly
    in recipe YAML (e.g. ``"true"`` -> ``True``, ``"4"`` -> ``4``).

    Args:
        fields: Dict of config fields to coerce.

    Returns:
        ``(coerced_fields, errors)`` where *errors* lists human-readable
        messages for any field that failed type coercion.
    """
    coerced, errors = get_native_schema_provider().coerce_config(fields)
    return coerced, [message for _, message in errors]
