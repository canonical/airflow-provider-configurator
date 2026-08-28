# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""End-to-end style tests: real INI -> template+secrets -> interface round-trip."""

import json

import airflow_provider_configurator as apc
import ops
import ops.testing
import pytest

from config_generator import build_template_and_secrets

RELATION_NAME = "airflow-provider-configuration"
RELATION_INTERFACE = "airflow_provider_configuration"

# A realistic, multi-section provider INI (non-sensitive values only).
SAMPLE_INI = """\
[databricks]
host = https://example.cloud.databricks.com

[aws]
region = us-east-1
"""

# Sensitive values, nested section -> option -> value, to be templated out.
SAMPLE_SENSITIVE = {
    "databricks": {"token": "dapi-super-secret"},
    "aws": {"secret_access_key": "aws-super-secret"},
}


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
            "provides": {RELATION_NAME: {"interface": RELATION_INTERFACE}},
        },
    )


class TestConfigGenerator:
    def test_placeholders_and_sensitive_map(self):
        template, sensitive = build_template_and_secrets(SAMPLE_INI, SAMPLE_SENSITIVE)

        # Non-sensitive values stay literal in the template.
        assert "host = https://example.cloud.databricks.com" in template
        assert "region = us-east-1" in template

        # Sensitive values become namespaced placeholders, not literals.
        assert "{{ databricks__token }}" in template
        assert "{{ aws__secret_access_key }}" in template
        assert "dapi-super-secret" not in template
        assert "aws-super-secret" not in template

        # The flat sensitive map is keyed by placeholder name.
        assert sensitive == {
            "databricks__token": "dapi-super-secret",
            "aws__secret_access_key": "aws-super-secret",
        }

    def test_no_sensitive_data(self):
        template, sensitive = build_template_and_secrets(SAMPLE_INI, None)
        assert "host = https://example.cloud.databricks.com" in template
        assert sensitive == {}


class TestFullRoundTrip:
    def test_ini_flows_through_the_relation(self, provider_context):
        """Real INI -> config_generator -> set_configuration -> databag + secret."""
        template, sensitive = build_template_and_secrets(SAMPLE_INI, SAMPLE_SENSITIVE)

        relation = ops.testing.Relation(RELATION_NAME, interface=RELATION_INTERFACE)
        state = ops.testing.State(leader=True, relations=[relation])

        with provider_context(provider_context.on.relation_changed(relation), state) as manager:
            manager.charm.provider.set_configuration(
                provider_configuration=template,
                provider_configuration_sensitive_data=sensitive,
            )
            state_out = manager.run()

        out_relation = state_out.get_relation(relation.id)

        # The template (with placeholders) is on the databag; secrets are not.
        published = out_relation.local_app_data["provider-configuration"]
        assert "{{ databricks__token }}" in published
        assert "dapi-super-secret" not in published

        # The sensitive values live in the charm secret, JSON-encoded.
        secret_id = out_relation.local_app_data["provider-configuration-secret-id"]
        assert state_out.get_secret(id=secret_id).latest_content == {
            "sensitive-data": json.dumps(sensitive)
        }
