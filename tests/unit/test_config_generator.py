# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""End-to-end style tests: real INI -> template+secrets -> interface round-trip."""

import json

import ops.testing

from config_generator import build_template_and_secrets

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


class TestConfigGenerator:
    def test_placeholders_and_sensitive_map(self):
        template, sensitive = build_template_and_secrets(SAMPLE_INI, SAMPLE_SENSITIVE)

        # Non-sensitive values stay literal in the template.
        assert "host = https://example.cloud.databricks.com" in template
        assert "region = us-east-1" in template

        # Sensitive values become namespaced placeholders, not literals.
        assert "{{ provider__databricks__token }}" in template
        assert "{{ provider__aws__secret_access_key }}" in template
        assert "dapi-super-secret" not in template
        assert "aws-super-secret" not in template

        # The flat sensitive map is keyed by placeholder name.
        assert sensitive == {
            "provider__databricks__token": "dapi-super-secret",
            "provider__aws__secret_access_key": "aws-super-secret",
        }

    def test_no_sensitive_data(self):
        template, sensitive = build_template_and_secrets(SAMPLE_INI, None)
        assert "host = https://example.cloud.databricks.com" in template
        assert sensitive == {}


class TestFullRoundTrip:
    def test_ini_flows_through_the_relation(self, provider_context):
        """Real INI -> config_generator -> set_configuration -> databag + secret."""
        template, sensitive = build_template_and_secrets(SAMPLE_INI, SAMPLE_SENSITIVE)

        relation = ops.testing.Relation(
            "airflow-provider-configuration", interface="airflow_provider_configuration"
        )
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
        assert "{{ provider__databricks__token }}" in published
        assert "dapi-super-secret" not in published

        # The sensitive values live in the charm secret, JSON-encoded.
        secret_uri = out_relation.local_app_data["provider-configuration-secret-uri"]
        assert state_out.get_secret(id=secret_uri).latest_content == {
            "sensitive-data": json.dumps(sensitive)
        }
