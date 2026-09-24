#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Parsing of the user-supplied sensitive provider configuration.

The operator supplies sensitive provider values through a Juju user secret (the
`airflow_provider_configurations_secret` config option). The secret holds a JSON
string under the key `airflow_provider_configurations`, structured as a map of
provider name to a nested section/option map, e.g.::

    {"databricks": {"databricks": {"token": "dapi-xxx"}},
     "amazon": {"aws": {"secret_access_key": "yyy"}}}

The provider-name nesting is for the admin's authoring convenience only; this
module flattens it away (spec 1.1), producing a nested section/option/value map
suitable for the config generator. Two providers setting the same section.option
is a prohibited collision (spec 3.3) and raises DuplicateSensitiveKeyError.
"""

import json

# The key inside the user secret whose value is the JSON payload.
SENSITIVE_CONFIG_SECRET_KEY = "airflow_provider_configurations"


class DuplicateSensitiveKeyError(Exception):
    """Raised when two providers set the same section.option sensitive value.

    The charm cannot decide which value should prevail, so the unit must block
    (spec 3.3).
    """


class InvalidSensitiveConfigError(Exception):
    """Raised when the sensitive config payload does not have the expected shape.

    The payload must be a JSON object of provider -> section -> option -> value,
    i.e. three levels of nested objects with string leaf values. Anything else
    (a list, a scalar, a wrongly-nested map) is a malformed secret and the unit
    must block.
    """


def parse_sensitive_config(raw_json: str) -> dict[str, dict[str, str]]:
    """Parse and flatten the user secret's JSON payload.

    Args:
        raw_json: the JSON string stored under SENSITIVE_CONFIG_SECRET_KEY, a map
            of provider -> section -> option -> value.

    Returns:
        A nested section -> option -> value map with the provider key dropped.
        Empty if the payload is empty.

    Raises:
        DuplicateSensitiveKeyError: if two providers set the same section.option.
        InvalidSensitiveConfigError: if the payload is not shaped as a
            provider -> section -> option -> value map of strings.
        json.JSONDecodeError: if the payload is not valid JSON.
    """
    nested = json.loads(raw_json) if raw_json else {}
    if not isinstance(nested, dict):
        raise InvalidSensitiveConfigError(
            "Sensitive configuration must be a JSON object of "
            "provider -> section -> option -> value."
        )

    flattened: dict[str, dict[str, str]] = {}
    for provider, sections in nested.items():
        if not isinstance(sections, dict):
            raise InvalidSensitiveConfigError(
                f"Provider '{provider}' must map to an object of section -> option -> value."
            )
        for section, options in sections.items():
            if not isinstance(options, dict):
                raise InvalidSensitiveConfigError(
                    f"Section '{section}' must map to an object of option -> value."
                )
            for option, value in options.items():
                if not isinstance(value, str):
                    raise InvalidSensitiveConfigError(
                        f"Value for {section}.{option} must be a string."
                    )
                section_map = flattened.setdefault(section, {})
                if option in section_map:
                    raise DuplicateSensitiveKeyError(
                        f"Two providers set the same sensitive value for {section}.{option}"
                    )
                section_map[option] = value
    return flattened
