#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Layer 2 validation: a small, static denylist over provider configuration.

Implements spec WF029 section 3.2.2. A short, team-maintained list (shipped in
``denylist.yaml``) of whole sections and individual ``section.option`` keys that
a provider must never set, regardless of what the coordinator currently renders
-- the residual case the coordinator's dynamic Layer 1 collision detection
cannot catch, because Layer 1 only knows what is *currently* rendered, not what
is *always* out of bounds.

Why the shipped list is section-shaped, and what it deliberately leaves to
Layer 1, is recorded in documentation/adr/0001-layer-2-denylist-scope.md.

``apply_denylist`` drops any denied key from BOTH the non-sensitive .ini and the
sensitive-data map (a denied option could be supplied either way), returning the
filtered inputs plus the list of dropped keys so the caller can log and surface a
status message. Dropping is non-blocking (spec 3.4).
"""

import configparser
import io
from pathlib import Path

import yaml

# denylist.yaml is shipped alongside this module in the charm (src/).
DENYLIST_PATH = Path(__file__).parent / "denylist.yaml"

# Airflow resolves `<option>_cmd` by running its value as a shell command, and
# `<option>_secret` by reading it from the configured secrets backend. Both are
# accepted in place of the bare option, so denying `core.fernet_key` without
# these would leave `core.fernet_key_cmd` -- a straight command-execution
# primitive -- wide open. Layer 1 cannot cover them either: the coordinator
# renders the bare option, so a `_cmd` variant collides with nothing.
SENSITIVE_VALUE_SUFFIXES = ("_cmd", "_secret")


class DenylistUnavailableError(Exception):
    """The shipped denylist could not be loaded, so Layer 2 cannot be enforced.

    Not a violation in the sense of spec 3.4: there is no offending key, because
    the charm cannot judge any key at all. Treated like a missing config file
    (spec 1.3) instead -- letting it pass would publish provider configuration
    with no Layer 2 validation and nothing to say so, so the charm blocks.
    """


def load_denylist(path: Path | None = None) -> frozenset[str]:
    """Return the set of denied entries from the denylist file.

    Args:
        path: location of the denylist YAML. Defaults to the shipped one,
            resolved at call time so tests can patch ``DENYLIST_PATH``; a
            default argument would bind it at import and ignore the patch.

    Returns:
        A frozenset of entries, each either a bare ``"section"`` or a
        ``"section.option"`` string.

    Raises:
        DenylistUnavailableError: if the file is missing, unreadable, not valid
            YAML, or has no non-empty ``deny`` list. The denylist ships with the
            charm, so any of these means a broken charm, not Layer 2 switched
            off.
    """
    path = DENYLIST_PATH if path is None else path
    try:
        data = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as e:
        raise DenylistUnavailableError(f"cannot read denylist at {path}: {e}") from e

    entries = (data or {}).get("deny") if isinstance(data, dict) else None
    if not entries or not isinstance(entries, list):
        raise DenylistUnavailableError(f"denylist at {path} has no non-empty 'deny' list")
    return frozenset(entries)


def _key(section: str, option: str) -> str:
    """Return the ``section.option`` key used for denylist lookups."""
    return f"{section}.{option}"


def _matches(candidate: str, denied: frozenset[str]) -> bool:
    """Return whether ``candidate`` is denied, allowing for case and suffixes.

    An entry without a ``.`` denies a whole section, so every option in it is
    matched without having to be enumerated. That is how the shipped list stays
    short enough to review (spec 3.2.2) while still covering options added by a
    future Airflow release.

    Matching is case-insensitive, for section names as much as option names:
    Airflow lowercases both when it reads airflow.cfg, so ``[CORE] DAGS_FOLDER``
    and ``[core] dags_folder`` are the same setting to Airflow. A case-sensitive
    denylist would be bypassed with the shift key while Airflow still honoured
    the value. A denied option also covers its ``_cmd`` / ``_secret`` variants
    (see SENSITIVE_VALUE_SUFFIXES).

    Args:
        candidate: a ``section.option`` key or a bare option name.
        denied: the matching denied set, already lowercased.
    """
    candidate = candidate.lower()
    # Only for a `section.option` candidate: a bare option name (the [DEFAULT]
    # case) has no section half to test, and must not be read as one.
    section, dot, _ = candidate.partition(".")
    if dot and section in denied:
        return True
    if candidate in denied:
        return True
    return any(
        candidate.endswith(suffix) and candidate[: -len(suffix)] in denied
        for suffix in SENSITIVE_VALUE_SUFFIXES
    )


def _denied_option_names(denylist: frozenset[str]) -> frozenset[str]:
    """Return the lowercased option half of every denied ``section.option`` key.

    Used for [DEFAULT], where an option applies under whatever section names the
    file happens to use, so the section half cannot be matched on. Whole-section
    entries contribute nothing here and are skipped; [DEFAULT] against those is
    handled separately in ``_filter_ini``.
    """
    return frozenset(key.split(".", 1)[1].lower() for key in denylist if "." in key)


def _filter_ini(non_sensitive_ini: str, denylist: frozenset[str], dropped: set[str]) -> str:
    """Return the INI with denied keys removed, recording drops in ``dropped``.

    Args:
        non_sensitive_ini: the INI string synced from git.
        denylist: the set of denied ``section.option`` keys, already lowercased.
        dropped: mutated in place with every key that was removed.

    Returns:
        The filtered INI, re-serialised with a case-preserving
        ``RawConfigParser`` (matching ``config_generator``). Sections left with
        no options of their own are removed.
    """
    parser = configparser.RawConfigParser()
    # Preserve option-name case, matching config_generator.build_template_and_secrets.
    # Matching compensates for this by lowercasing on comparison (see _matches).
    parser.optionxform = str  # type: ignore[assignment, method-assign]
    parser.read_string(non_sensitive_ini)

    # [DEFAULT] options are inherited by every section, including sections added
    # later, and configparser cannot remove an inherited default from an
    # individual section -- remove_option() would report success while write()
    # re-emitted the value. So denied options must be dropped at the source.
    denied_option_names = _denied_option_names(denylist)
    for option in list(parser.defaults()):
        # Two ways a default is denied: [DEFAULT] itself is a denied section, or
        # the option name is denied under some section (so it would be denied
        # wherever the default landed).
        if _matches(_key(configparser.DEFAULTSECT, option), denylist) or _matches(
            option, denied_option_names
        ):
            parser.remove_option(configparser.DEFAULTSECT, option)
            dropped.add(_key(configparser.DEFAULTSECT, option))

    # Recomputed after the pruning above, and used to tell a section's own
    # options from the ones it merely inherits: parser.options() returns both.
    defaults = set(parser.defaults())
    for section in parser.sections():
        own_options = set(parser.options(section)) - defaults
        for option in sorted(own_options):
            if _matches(_key(section, option), denylist):
                parser.remove_option(section, option)
                dropped.add(_key(section, option))
        # A section emptied by the drop should not linger as a bare header.
        if not set(parser.options(section)) - defaults:
            parser.remove_section(section)

    buffer = io.StringIO()
    parser.write(buffer)
    return buffer.getvalue()


def _filter_sensitive(
    sensitive_data: dict[str, dict[str, str]],
    denylist: frozenset[str],
    dropped: set[str],
) -> dict[str, dict[str, str]]:
    """Return the sensitive map with denied keys removed.

    A denied option could just as easily be supplied through the Juju secret as
    through the .ini, so Layer 2 has to cover both inputs.

    Args:
        sensitive_data: the nested ``{section: {option: value}}`` map.
        denylist: the set of denied ``section.option`` keys, already lowercased.
        dropped: mutated in place with every key that was removed.

    Returns:
        The filtered map, with sections left empty by a drop omitted.
    """
    filtered: dict[str, dict[str, str]] = {}
    for section, options in sensitive_data.items():
        kept = {}
        for option, value in options.items():
            if _matches(_key(section, option), denylist):
                dropped.add(_key(section, option))
            else:
                kept[option] = value
        if kept:
            filtered[section] = kept
    return filtered


def apply_denylist(
    non_sensitive_ini: str,
    sensitive_data: dict[str, dict[str, str]],
    denylist: frozenset[str] | None = None,
) -> tuple[str, dict[str, dict[str, str]], list[str]]:
    """Drop denied ``section.option`` keys from both provider inputs.

    Args:
        non_sensitive_ini: the INI string synced from git.
        sensitive_data: the nested ``{section: {option: value}}`` map from the
            user secret.
        denylist: the set of denied sections and ``section.option`` keys; loaded
            from ``denylist.yaml`` when not supplied (overridable for testing).
            Matched case-insensitively.

    Returns:
        A tuple of ``(filtered_ini, filtered_sensitive, dropped_keys)`` where
        ``dropped_keys`` is the sorted, de-duplicated list of ``section.option``
        keys that were removed from either input, spelled as they appeared in
        the input so the operator recognises what was dropped.

    Raises:
        DenylistUnavailableError: if ``denylist`` was not supplied and the
            shipped file could not be loaded.
    """
    if denylist is None:
        denylist = load_denylist()
    # Normalised once here so the filters can compare directly.
    denylist = frozenset(key.strip().lower() for key in denylist)

    dropped: set[str] = set()
    filtered_ini = _filter_ini(non_sensitive_ini, denylist, dropped)
    filtered_sensitive = _filter_sensitive(sensitive_data, denylist, dropped)

    return filtered_ini, filtered_sensitive, sorted(dropped)
