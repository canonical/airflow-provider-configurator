"""Tests for Provider Configurator security-event logging."""

import json
import logging
from unittest.mock import Mock, patch

import ops
import ops.testing

from charm import AirflowProviderConfiguratorCharm
from security_logging import ProviderSecurityLogger

TARGET = "airflow-provider-configurator"


def _event_from_log(caplog) -> dict:
    record = next(record for record in caplog.records if record.name == "security_logging")
    return json.loads(record.getMessage())["owasp_event"]


def test_configuration_change_emits_authz_admin_with_context(caplog):
    security_logger = ProviderSecurityLogger(container=Mock(), target=TARGET)

    with caplog.at_level(logging.WARNING, logger="security_logging"):
        security_logger.log_configuration_change(
            "update_provider_configuration",
            {"configuration_fields": ["core.executor"], "source": "git_repository"},
        )

    event = _event_from_log(caplog)
    assert event["appid"] == "airflow.provider-configurator"
    assert event["event"] == "authz_admin:unattributed,update_provider_configuration"
    assert event["level"] == "WARN"
    assert event["actor"] == "unattributed"
    assert event["target"] == TARGET
    assert "action" not in event
    assert "outcome" not in event
    assert event["data"] == {
        "configuration_fields": ["core.executor"],
        "source": "git_repository",
    }


def test_denied_key_emits_input_validation_fail_at_warn_without_value(caplog):
    security_logger = ProviderSecurityLogger(container=Mock(), target=TARGET)

    with caplog.at_level(logging.WARNING, logger="security_logging"):
        security_logger.log_denied_key("api.secret_key")

    event = _event_from_log(caplog)
    assert event["event"] == "input_validation_discrete_fail:api.secret_key,unattributed"
    assert event["level"] == "WARN"
    assert event["actor"] == "unattributed"
    assert event["data"] == {
        "actor_attribution": "repository_audit_log",
        "config_key": "api.secret_key",
    }
    assert "secret-value" not in json.dumps(event)


def test_denylist_events_are_deduplicated_until_drop_set_changes(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    ini = repo_dir / "providers.ini"
    container = ops.testing.Container(
        name="git-sync",
        can_connect=True,
        mounts={"content": ops.testing.Mount(location="/git/repo", source=repo_dir)},
    )
    git_relation = ops.testing.Relation(
        "remote-airflow-provider-configurations",
        interface="git",
        remote_app_data={
            "repository-url": "https://github.com/example/provider-config",
            "tracking-ref": "main",
        },
    )
    state = ops.testing.State(
        leader=True,
        containers=[container],
        relations=[
            git_relation,
            ops.testing.Relation(
                "airflow-provider-configuration", interface="airflow_provider_configuration"
            ),
            ops.testing.PeerRelation(
                "replicas", interface="airflow_provider_configurator_replica"
            ),
        ],
        config={"airflow_provider_configurations_file_path": "providers.ini"},
    )
    context = ops.testing.Context(charm_type=AirflowProviderConfiguratorCharm)
    logged: list[str] = []

    def run(ini_text: str) -> None:
        nonlocal state
        ini.write_text(ini_text)
        with patch("security_logging.ProviderSecurityLogger.log_denied_key") as mock_log:
            state = context.run(context.on.update_status(), state)
        logged.extend(call.args[0] for call in mock_log.call_args_list)

    run("[gcs]\nconn_id = x\n[core]\ndags_folder = /tmp/evil\n")
    run("[gcs]\nconn_id = x\n[core]\ndags_folder = /tmp/evil\n")
    run("[gcs]\nconn_id = x\n[core]\nplugins_folder = /tmp/evil\n")

    assert logged == ["core.dags_folder", "core.plugins_folder"]
