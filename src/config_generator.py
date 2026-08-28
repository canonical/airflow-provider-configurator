#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Conversion of provider INI configuration into a Jinja2 template plus secrets.

Implements the transformation described in spec section 1.5: given a provider
configuration INI file (non-sensitive) and a map of sensitive values, produce:

  * a Jinja2 template string where each sensitive value is replaced by a
    `{{ section__option }}` placeholder, and
  * a flat sensitive-data map of `section__option` -> value

The airflow-coordinator later renders the template using the sensitive map.

This module intentionally lives on the charm side (src/), not in the interface
library, mirroring the coordinator's own config_generator module.
"""

import configparser
import io


def _placeholder_name(section: str, option: str) -> str:
    """Return the namespaced placeholder name for a section.option pair.

    For example, section "gcs" and option "conn_id" yields "gcs__conn_id".
    """
    return f"{section}__{option}"


def build_template_and_secrets(
    non_sensitive_ini: str,
    sensitive_data: dict[str, dict[str, str]] | None = None,
) -> tuple[str, dict[str, str]]:
    """Build a Jinja2 template and a flat sensitive-data map from INI input.

    Args:
        non_sensitive_ini: the INI string containing non-sensitive provider
            configuration (each value is used literally in the template).
        sensitive_data: a nested map of section -> option -> value for sensitive
            values. Each such value is written into the template as a
            `{{ section__option }}` placeholder rather than its literal value,
            and returned in the flat sensitive map.

    Returns:
        A tuple of (jinja_template_string, flat_sensitive_map) where the flat map
        is keyed by placeholder name (e.g. "gcs__conn_id").
    """
    sensitive_data = sensitive_data or {}

    parser = configparser.RawConfigParser()
    # Preserve option name case; INI options are case-insensitive by default.
    parser.optionxform = str
    parser.read_string(non_sensitive_ini)

    flat_sensitive: dict[str, str] = {}

    # Merge sensitive options into the parser as placeholder references.
    for section, options in sensitive_data.items():
        if not parser.has_section(section):
            parser.add_section(section)
        for option, value in options.items():
            placeholder = _placeholder_name(section, option)
            parser.set(section, option, f"{{{{ {placeholder} }}}}")
            flat_sensitive[placeholder] = value

    buffer = io.StringIO()
    parser.write(buffer)
    return buffer.getvalue(), flat_sensitive
