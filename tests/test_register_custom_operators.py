# -*- coding: utf-8 -*-
"""Tests for the register_custom_operators tool logic.

Uses the real DJ environment (no mocks) to verify:
1. Registration returns the correct operator name list.
2. Empty paths return an error.
3. Repeated registration is idempotent.
4. After registration, build_process_spec validation passes.
"""

from __future__ import annotations

import sys
import textwrap
import uuid

import pytest

from data_juicer_agents.tools.dev.register_custom_operators.logic import (
    register_custom_operators,
)
from data_juicer_agents.tools.plan.build_process_spec.logic import (
    build_process_spec,
)


def _unique_op_name(prefix: str) -> str:
    """Generate a unique operator name to avoid registry collisions."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _write_custom_op(directory, operator_name: str, class_name: str) -> str:
    """Write a minimal custom Mapper op file and return its path.

    DJ's ``load_custom_operators`` imports the package via
    ``importlib.import_module(package_name)`` which only executes
    ``__init__.py``.  So the ``__init__.py`` must explicitly import
    the op module for the ``@OPERATORS.register_module`` decorator
    to fire.
    """
    op_file = directory / f"{operator_name}.py"
    op_file.write_text(
        textwrap.dedent(f"""\
            from data_juicer.ops.base_op import Mapper, OPERATORS

            @OPERATORS.register_module("{operator_name}")
            class {class_name}(Mapper):
                def __init__(self, threshold: float = 0.5, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    self.threshold = threshold

                def process_single(self, sample):
                    return sample
        """),
        encoding="utf-8",
    )

    # Update __init__.py to import this module so the decorator fires
    init_file = directory / "__init__.py"
    existing = init_file.read_text(encoding="utf-8") if init_file.exists() else ""
    import_line = f"from . import {operator_name}\n"
    if import_line not in existing:
        init_file.write_text(existing + import_line, encoding="utf-8")

    return str(op_file)


class TestRegisterCustomOperators:
    """Tests for the register_custom_operators logic function."""

    def test_register_returns_operator_names(self, tmp_path):
        """Registering a valid custom op returns its name in the list."""
        pkg = f"pkg_reg_{uuid.uuid4().hex[:8]}"
        path = tmp_path / pkg
        path.mkdir()

        op_name = _unique_op_name("test_reg_op")
        _write_custom_op(path, op_name, "RegTestOp")

        # Add tmp_path to sys.path so DJ can import the package
        sys.path.insert(0, str(tmp_path))
        try:
            result = register_custom_operators(paths=[str(path)])
        finally:
            sys.path.remove(str(tmp_path))

        assert result["ok"] is True, f"Registration failed: {result}"
        assert op_name in result["registered_operators"], (
            f"Expected '{op_name}' in registered_operators, got: {result['registered_operators']}"
        )

    def test_empty_paths_returns_error(self):
        """Empty or whitespace-only paths must return an error."""
        result = register_custom_operators(paths=[])
        assert result["ok"] is False
        assert result["error_type"] == "missing_required"

        result2 = register_custom_operators(paths=["", "  "])
        assert result2["ok"] is False

    def test_repeated_registration_is_idempotent(self, tmp_path):
        """Calling register twice with the same path should not error."""
        pkg = f"pkg_idem_{uuid.uuid4().hex[:8]}"
        path = tmp_path / pkg
        path.mkdir()

        op_name = _unique_op_name("test_idem_op")
        _write_custom_op(path, op_name, "IdempotentOp")

        sys.path.insert(0, str(tmp_path))
        try:
            result1 = register_custom_operators(paths=[str(path)])
            assert result1["ok"] is True

            result2 = register_custom_operators(paths=[str(path)])
            assert result2["ok"] is True
            # The op should still be in the list (it's a global diff)
            assert op_name in result2["registered_operators"]
        finally:
            sys.path.remove(str(tmp_path))

    def test_registered_op_passes_build_process_spec(self, tmp_path):
        """After registration, build_process_spec should validate the op."""
        pkg = f"pkg_build_{uuid.uuid4().hex[:8]}"
        path = tmp_path / pkg
        path.mkdir()

        op_name = _unique_op_name("test_build_op")
        _write_custom_op(path, op_name, "BuildTestOp")

        sys.path.insert(0, str(tmp_path))
        try:
            reg_result = register_custom_operators(paths=[str(path)])
            assert reg_result["ok"] is True

            # Now build_process_spec should accept this operator.
            # Note: we use empty params because DJ's parser-based param
            # validation may not recognise __init__ params from simple
            # custom ops (it relies on class docstrings / jsonargparse).
            build_result = build_process_spec(
                operators=[{"name": op_name, "params": {}}],
                custom_operator_paths=[str(path)],
            )
            assert build_result["ok"] is True, (
                f"build_process_spec failed after registration: {build_result}"
            )
            assert op_name in build_result["operator_names"]
        finally:
            sys.path.remove(str(tmp_path))

    def test_register_calls_refresh_op_catalog(self, tmp_path, monkeypatch):
        """After successful registration, refresh_op_catalog is invoked."""
        refresh_called = {"count": 0}

        def fake_refresh():
            refresh_called["count"] += 1
            return True

        # Patch at the source module — the logic function imports it
        # inside the function body via:
        #   from data_juicer_agents.tools.retrieve._shared.backend import refresh_op_catalog
        monkeypatch.setattr(
            "data_juicer_agents.tools.retrieve._shared.backend.refresh_op_catalog",
            fake_refresh,
        )

        pkg = f"pkg_refresh_{uuid.uuid4().hex[:8]}"
        path = tmp_path / pkg
        path.mkdir()

        op_name = _unique_op_name("test_refresh_op")
        _write_custom_op(path, op_name, "RefreshTestOp")

        sys.path.insert(0, str(tmp_path))
        try:
            result = register_custom_operators(paths=[str(path)])
            assert result["ok"] is True
            assert op_name in result["registered_operators"]
            assert refresh_called["count"] == 1, (
                f"Expected refresh_op_catalog to be called once, got {refresh_called['count']}"
            )
        finally:
            sys.path.remove(str(tmp_path))

    def test_register_refresh_failure_does_not_break_registration(self, tmp_path, monkeypatch):
        """If refresh_op_catalog fails, registration still succeeds."""
        def failing_refresh():
            raise RuntimeError("catalog refresh boom")

        monkeypatch.setattr(
            "data_juicer_agents.tools.retrieve._shared.backend.refresh_op_catalog",
            failing_refresh,
        )

        pkg = f"pkg_rfail_{uuid.uuid4().hex[:8]}"
        path = tmp_path / pkg
        path.mkdir()

        op_name = _unique_op_name("test_rfail_op")
        _write_custom_op(path, op_name, "RFailTestOp")

        sys.path.insert(0, str(tmp_path))
        try:
            result = register_custom_operators(paths=[str(path)])
            # Registration should still succeed even if refresh fails
            assert result["ok"] is True
            assert op_name in result["registered_operators"]
        finally:
            sys.path.remove(str(tmp_path))

    def test_unregistered_op_fails_build_with_hint(self):
        """An unregistered custom op should fail with a helpful hint."""
        fake_op = _unique_op_name("never_registered_op")
        build_result = build_process_spec(
            operators=[{"name": fake_op, "params": {}}],
            custom_operator_paths=[],
        )
        assert build_result["ok"] is False
        # Check that the error message includes the registration hint
        error_text = " ".join(build_result.get("validation_errors", []))
        assert "register_custom_operators" in error_text, (
            f"Expected registration hint in error, got: {build_result['validation_errors']}"
        )
