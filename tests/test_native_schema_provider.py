# -*- coding: utf-8 -*-

from data_juicer_agents.native_schema import get_native_schema_provider


def test_provider_exposes_system_and_dataset_schema():
    provider = get_native_schema_provider()

    system_schema = provider.get_system_schema()
    dataset_schema = provider.get_dataset_schema()

    assert "executor_type" in system_schema
    assert "np" in system_schema
    assert "dataset_path" in dataset_schema
    assert "export_path" in dataset_schema
    assert all(item.native_group == "system" for item in system_schema.values())
    assert all(item.native_group == "dataset" for item in dataset_schema.values())


def test_provider_reports_unclassified_fields_without_agent_managed_values():
    provider = get_native_schema_provider()

    unclassified = provider.list_unclassified_fields()

    assert "project_name" in unclassified
    assert "job_id" in unclassified
    assert "config" not in provider.get_global_schema()


def test_provider_validate_dataset_config_reuses_load_strategy_rules(tmp_path):
    provider = get_native_schema_provider()
    export_path = tmp_path / "out" / "result.jsonl"
    export_path.parent.mkdir(parents=True, exist_ok=True)

    result = provider.validate_dataset_config(
        {
            "dataset": {
                "configs": [{"type": "local"}],
            },
            "export_path": str(export_path),
        }
    )

    assert result.ok is False
    assert any("path" in message for message in result.error_messages())


def test_provider_validate_process_config_flags_unknown_operator_and_param():
    provider = get_native_schema_provider()

    result = provider.validate_process_config(
        [
            {"not_a_real_operator": {}},
            {"text_length_filter": {"not_a_real_param": 1}},
        ]
    )

    assert result.ok is False
    assert any("unknown operator" in message for message in result.error_messages())
    assert any("unknown param" in message for message in result.error_messages())
