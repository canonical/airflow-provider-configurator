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
        json.JSONDecodeError: if the payload is not valid JSON.
    """
    nested = json.loads(raw_json) if raw_json else {}

    flattened: dict[str, dict[str, str]] = {}
    for _provider, sections in nested.items():
        for section, options in sections.items():
            for option, value in options.items():
                existing = flattened.get(section, {})
                if option in existing:
                    raise DuplicateSensitiveKeyError(
                        f"Two providers set the same sensitive value for {section}.{option}"
                    )
                flattened.setdefault(section, {})[option] = value
    return flattened
