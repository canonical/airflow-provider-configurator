# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the airflow_provider_configuration relation interface library."""

import json

import airflow_provider_configurator as apc
import ops
import ops.testing
import pytest

RELATION_NAME = "airflow-provider-configuration"
RELATION_INTERFACE = "airflow_provider_configuration"

SAMPLE_TEMPLATE = "[gcs]\nconn_id = {{ gcs__conn_id }}\n"
SAMPLE_SENSITIVE = {"gcs__conn_id": "s3cret"}


class ProviderHarnessCharm(ops.CharmBase):
    """Mock charm wiring the provider side of the interface."""

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = apc.AirflowProviderConfigurationProvides(self, RELATION_NAME)


class RequirerHarnessCharm(ops.CharmBase):
    """Mock charm wiring the requirer side of the interface."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = apc.AirflowProviderConfigurationRequires(self, RELATION_NAME)


@pytest.fixture
def provider_context():
    return ops.testing.Context(
        charm_type=ProviderHarnessCharm,
        meta={
            "name": "airflow-provider-configurator",
            "provides": {
                RELATION_NAME: {"interface": RELATION_INTERFACE},
            },
        },
    )


@pytest.fixture
def requirer_context():
    return ops.testing.Context(
        charm_type=RequirerHarnessCharm,
        meta={
            "name": "airflow-coordinator",
            "requires": {
                RELATION_NAME: {"interface": RELATION_INTERFACE, "limit": 1},
            },
        },
    )


@pytest.fixture
def relation():
    return ops.testing.Relation(RELATION_NAME, interface=RELATION_INTERFACE)


class TestProvides:
    def test_set_configuration_writes_databag_and_secret(self, provider_context, relation):
        state = ops.testing.State(leader=True, relations=[relation])
        with provider_context(provider_context.on.relation_changed(relation), state) as manager:
            manager.charm.provider.set_configuration(
                provider_configuration=SAMPLE_TEMPLATE,
                provider_configuration_sensitive_data=SAMPLE_SENSITIVE,
            )
            state_out = manager.run()

        out_relation = state_out.get_relation(relation.id)
        assert out_relation.local_app_data["provider-configuration"] == SAMPLE_TEMPLATE

        secret_id = out_relation.local_app_data["provider-configuration-secret-id"]
        assert secret_id
        assert state_out.get_secret(id=secret_id).latest_content == {
            "sensitive-data": json.dumps(SAMPLE_SENSITIVE)
        }

    def test_set_configuration_noop_when_not_leader(self, provider_context, relation):
        state = ops.testing.State(leader=False, relations=[relation])
        with provider_context(provider_context.on.relation_changed(relation), state) as manager:
            manager.charm.provider.set_configuration(
                provider_configuration=SAMPLE_TEMPLATE,
                provider_configuration_sensitive_data=SAMPLE_SENSITIVE,
            )
            state_out = manager.run()

        out_relation = state_out.get_relation(relation.id)
        assert "provider-configuration" not in out_relation.local_app_data

    def test_set_configuration_noop_when_no_relation(self, provider_context):
        state = ops.testing.State(leader=True, relations=[])
        with provider_context(provider_context.on.update_status(), state) as manager:
            # Should not raise even though there is no relation to write to.
            manager.charm.provider.set_configuration(
                provider_configuration=SAMPLE_TEMPLATE,
                provider_configuration_sensitive_data=SAMPLE_SENSITIVE,
            )
            manager.run()

    def test_clear_configuration_clears_databag(self, provider_context, relation):
        state = ops.testing.State(leader=True, relations=[relation])
        with provider_context(provider_context.on.relation_changed(relation), state) as manager:
            manager.charm.provider.set_configuration(
                provider_configuration=SAMPLE_TEMPLATE,
                provider_configuration_sensitive_data=SAMPLE_SENSITIVE,
            )
            manager.charm.provider.clear_configuration()
            state_out = manager.run()

        out_relation = state_out.get_relation(relation.id)
        assert not out_relation.local_app_data.get("provider-configuration")


class TestRequires:
    def _remote_data_with_secret(self, requirer_context):
        """Build a secret + remote_app_data as the provider would have written."""
        secret = ops.testing.Secret({"sensitive-data": json.dumps(SAMPLE_SENSITIVE)})
        remote_data = {
            "provider-configuration": SAMPLE_TEMPLATE,
            "provider-configuration-secret-id": secret.id,
        }
        rel = ops.testing.Relation(
            RELATION_NAME,
            interface=RELATION_INTERFACE,
            remote_app_data=remote_data,
        )
        return secret, rel

    def test_configurations_returns_template(self, requirer_context):
        secret, rel = self._remote_data_with_secret(requirer_context)
        state = ops.testing.State(leader=True, relations=[rel], secrets=[secret])
        with requirer_context(requirer_context.on.relation_changed(rel), state) as manager:
            manager.run()
            assert manager.charm.requirer.configurations() == SAMPLE_TEMPLATE

    def test_get_sensitive_data_returns_secret_content(self, requirer_context):
        secret, rel = self._remote_data_with_secret(requirer_context)
        state = ops.testing.State(leader=True, relations=[rel], secrets=[secret])
        with requirer_context(requirer_context.on.relation_changed(rel), state) as manager:
            manager.run()
            assert manager.charm.requirer.get_sensitive_data() == SAMPLE_SENSITIVE

    def test_configuration_keys_returns_section_option_keys(self, requirer_context):
        secret, rel = self._remote_data_with_secret(requirer_context)
        state = ops.testing.State(leader=True, relations=[rel], secrets=[secret])
        with requirer_context(requirer_context.on.relation_changed(rel), state) as manager:
            manager.run()
            assert manager.charm.requirer.configuration_keys() == {"gcs.conn_id"}
