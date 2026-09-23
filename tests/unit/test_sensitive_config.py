# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the sensitive provider configuration parser."""

import json

import pytest

from sensitive_config import (
    DuplicateSensitiveKeyError,
    InvalidSensitiveConfigError,
    parse_sensitive_config,
)


class TestParseSensitiveConfig:
    def test_flattens_provider_key(self):
        """The provider-name nesting is dropped, keeping section/option/value."""
        raw = json.dumps(
            {
                "databricks": {"databricks": {"token": "dapi-xxx"}},
                "amazon": {"aws": {"secret_access_key": "yyy"}},
            }
        )
        result = parse_sensitive_config(raw)
        assert result == {
            "databricks": {"token": "dapi-xxx"},
            "aws": {"secret_access_key": "yyy"},
        }

    def test_merges_same_section_different_options(self):
        """Two providers writing different options in the same section merge fine."""
        raw = json.dumps(
            {
                "provider_a": {"logging": {"remote_base_log_folder": "s3://a"}},
                "provider_b": {"logging": {"remote_log_conn_id": "aws_default"}},
            }
        )
        result = parse_sensitive_config(raw)
        assert result == {
            "logging": {
                "remote_base_log_folder": "s3://a",
                "remote_log_conn_id": "aws_default",
            }
        }

    def test_duplicate_section_option_raises(self):
        """Two providers setting the SAME section.option is a collision (spec 3.3)."""
        raw = json.dumps(
            {
                "provider_a": {"core": {"fernet_key": "key-a"}},
                "provider_b": {"core": {"fernet_key": "key-b"}},
            }
        )
        with pytest.raises(DuplicateSensitiveKeyError, match="core.fernet_key"):
            parse_sensitive_config(raw)

    def test_empty_payload_returns_empty(self):
        assert parse_sensitive_config("") == {}
        assert parse_sensitive_config("{}") == {}

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            parse_sensitive_config("not json")

    def test_non_object_payload_raises(self):
        """A top-level payload that is not an object is malformed."""
        with pytest.raises(InvalidSensitiveConfigError, match="provider -> section"):
            parse_sensitive_config(json.dumps(["not", "an", "object"]))

    def test_provider_not_mapping_raises(self):
        """A provider that does not map to an object is malformed."""
        with pytest.raises(InvalidSensitiveConfigError, match="section -> option"):
            parse_sensitive_config(json.dumps({"databricks": "not-an-object"}))

    def test_section_not_mapping_raises(self):
        """A section that does not map to an object is malformed."""
        with pytest.raises(InvalidSensitiveConfigError, match="option -> value"):
            parse_sensitive_config(json.dumps({"databricks": {"databricks": "not-an-object"}}))

    def test_non_string_value_raises(self):
        """A leaf value that is not a string is malformed."""
        with pytest.raises(InvalidSensitiveConfigError, match="must be a string"):
            parse_sensitive_config(json.dumps({"databricks": {"databricks": {"token": 123}}}))
