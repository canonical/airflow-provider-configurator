# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the airflow_provider_configuration relation interface library."""

import json
from unittest.mock import patch

import ops
import ops.testing

RELATION_NAME = "airflow-provider-configuration"
RELATION_INTERFACE = "airflow_provider_configuration"

SAMPLE_TEMPLATE = "[gcs]\nconn_id = {{ gcs__conn_id }}\n"
SAMPLE_SENSITIVE = {"gcs__conn_id": "s3cret"}


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
            manager.charm.provider.set_configuration(
                provider_configuration=SAMPLE_TEMPLATE,
                provider_configuration_sensitive_data=SAMPLE_SENSITIVE,
            )
            manager.run()

    def test_set_configuration_no_revision_churn_on_identical_content(
        self, provider_context, relation
    ):
        """Re-publishing identical sensitive data must not rewrite the secret.

        Scenario models a secret as current state only (latest_content /
        tracked_content) with no revision history, so there is no state attribute
        that distinguishes "written once" from "written twice with identical
        content". We therefore assert on the behaviour directly: the guard must
        skip the redundant ops.Secret.set_content call on the second, identical
        publish. The observable content is also checked to stay correct.
        """
        state = ops.testing.State(leader=True, relations=[relation])
        with provider_context(provider_context.on.relation_changed(relation), state) as manager:
            # First call creates the secret.
            manager.charm.provider.set_configuration(
                provider_configuration=SAMPLE_TEMPLATE,
                provider_configuration_sensitive_data=SAMPLE_SENSITIVE,
            )
            # A second call with identical data must not rewrite the secret.
            with patch.object(ops.Secret, "set_content") as mock_set_content:
                manager.charm.provider.set_configuration(
                    provider_configuration=SAMPLE_TEMPLATE,
                    provider_configuration_sensitive_data=SAMPLE_SENSITIVE,
                )
                mock_set_content.assert_not_called()
            state_out = manager.run()

        # The secret content is still correct after the no-op second publish.
        out_relation = state_out.get_relation(relation.id)
        secret_id = out_relation.local_app_data["provider-configuration-secret-id"]
        assert state_out.get_secret(id=secret_id).latest_content == {
            "sensitive-data": json.dumps(SAMPLE_SENSITIVE)
        }

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

    def test_clear_configuration_noop_when_not_leader(self, provider_context, relation):
        state = ops.testing.State(leader=False, relations=[relation])
        with provider_context(provider_context.on.relation_changed(relation), state) as manager:
            # Should not raise even though this unit is not the leader.
            manager.charm.provider.clear_configuration()
            manager.run()


class TestRequires:
    def _remote_data_with_secret(self):
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
        secret, rel = self._remote_data_with_secret()
        state = ops.testing.State(leader=True, relations=[rel], secrets=[secret])
        with requirer_context(requirer_context.on.relation_changed(rel), state) as manager:
            manager.run()
            assert manager.charm.requirer.configurations() == SAMPLE_TEMPLATE

    def test_get_sensitive_data_returns_secret_content(self, requirer_context):
        secret, rel = self._remote_data_with_secret()
        state = ops.testing.State(leader=True, relations=[rel], secrets=[secret])
        with requirer_context(requirer_context.on.relation_changed(rel), state) as manager:
            manager.run()
            assert manager.charm.requirer.get_sensitive_data() == SAMPLE_SENSITIVE

    def test_configuration_keys_returns_section_option_keys(self, requirer_context):
        secret, rel = self._remote_data_with_secret()
        state = ops.testing.State(leader=True, relations=[rel], secrets=[secret])
        with requirer_context(requirer_context.on.relation_changed(rel), state) as manager:
            manager.run()
            assert manager.charm.requirer.configuration_keys() == {"gcs.conn_id"}

    def test_configuration_keys_preserves_case_and_percent(self, requirer_context):
        """Regression: options must keep case and tolerate % (no interpolation)."""
        template = "[logging]\nLog_Format = %(asctime)s %(message)s\n"
        secret = ops.testing.Secret({"sensitive-data": json.dumps({})})
        rel = ops.testing.Relation(
            RELATION_NAME,
            interface=RELATION_INTERFACE,
            remote_app_data={
                "provider-configuration": template,
                "provider-configuration-secret-id": secret.id,
            },
        )
        state = ops.testing.State(leader=True, relations=[rel], secrets=[secret])
        with requirer_context(requirer_context.on.relation_changed(rel), state) as manager:
            manager.run()
            # Case preserved (Log_Format, not log_format); no InterpolationSyntaxError.
            assert manager.charm.requirer.configuration_keys() == {"logging.Log_Format"}

    def test_configurations_returns_none_without_relation(self, requirer_context):
        state = ops.testing.State(leader=True, relations=[])
        with requirer_context(requirer_context.on.update_status(), state) as manager:
            manager.run()
            assert manager.charm.requirer.configurations() is None
            assert manager.charm.requirer.get_sensitive_data() == {}
            assert manager.charm.requirer.configuration_keys() == set()
