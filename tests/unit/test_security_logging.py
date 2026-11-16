"""Tests for Provider Configurator security-event logging."""

import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

from charm import AirflowProviderConfiguratorCharm
from security_logging import ProviderSecurityLogger


def _event_from_log(caplog) -> dict:
    record = next(record for record in caplog.records if record.name == "security_logging")
    return json.loads(record.getMessage())["owasp_event"]


def test_configuration_change_emits_authz_admin_with_context(caplog):
    charm = SimpleNamespace(
        app=SimpleNamespace(name="airflow-provider-configurator"),
        _container=Mock(),
    )
    security_logger = ProviderSecurityLogger(charm)

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
    assert event["target"] == "airflow-provider-configurator"
    assert "action" not in event
    assert "outcome" not in event
    assert event["data"] == {
        "configuration_fields": ["core.executor"],
        "source": "git_repository",
    }


def test_denied_key_emits_authz_fail_without_value(caplog):
    charm = SimpleNamespace(
        app=SimpleNamespace(name="airflow-provider-configurator"),
        _container=Mock(),
    )
    security_logger = ProviderSecurityLogger(charm)

    with caplog.at_level(logging.CRITICAL, logger="security_logging"):
        security_logger.log_denied_key("api.secret_key")

    event = _event_from_log(caplog)
    assert event["event"] == "authz_fail:unattributed,api.secret_key"
    assert event["level"] == "CRITICAL"
    assert event["actor"] == "unattributed"
    assert "action" not in event
    assert "outcome" not in event
    assert event["data"] == {
        "actor_attribution": "repository_audit_log",
        "config_key": "api.secret_key",
    }
    assert "secret-value" not in json.dumps(event)


def test_denylist_events_are_deduplicated_until_drop_set_changes():
    peer_app_data: dict[str, str] = {}
    security_events = Mock()
    app = "airflow-provider-configurator"
    charm = SimpleNamespace(
        unit=SimpleNamespace(is_leader=lambda: True),
        model=SimpleNamespace(
            get_relation=lambda relation_name: SimpleNamespace(data={app: peer_app_data})
        ),
        app=app,
        _security_events=security_events,
    )

    AirflowProviderConfiguratorCharm._log_denied_keys_once(charm, ["api.secret_key"])
    AirflowProviderConfiguratorCharm._log_denied_keys_once(charm, ["api.secret_key"])
    AirflowProviderConfiguratorCharm._log_denied_keys_once(charm, ["core.fernet_key"])

    assert [call.args[0] for call in security_events.log_denied_key.call_args_list] == [
        "api.secret_key",
        "core.fernet_key",
    ]
