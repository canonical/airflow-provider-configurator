# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the Airflow Provider Configurator charm (git input path)."""

import json

import charms.git_integrator.v0.git as git
import ops
import ops.testing
import pytest

from charm import AirflowProviderConfiguratorCharm

GIT_RELATION = "remote-airflow-provider-configurations"
PROVIDER_RELATION = "airflow-provider-configuration"
FILE_PATH_CONFIG = "airflow_provider_configurations_file_path"

SENSITIVE_SECRET_CONFIG = "airflow_provider_configurations_secret"
SENSITIVE_CONFIG_KEY = "airflow_provider_configurations"

# The secret content key git-integrator uses for the PAT.
PAT_SECRET_KEY = "credentials-personal-access-token"

SAMPLE_INI = """\
[gcs]
conn_id = default_gcp

[logging]
remote_logging = True
"""


@pytest.fixture
def context():
    return ops.testing.Context(charm_type=AirflowProviderConfiguratorCharm)


@pytest.fixture
def container():
    """A reachable git-sync container with no synced content."""
    return ops.testing.Container(name="git-sync", can_connect=True)


@pytest.fixture
def synced_container(tmp_path):
    """A git-sync container with a synced provider .ini mounted at /git/repo.

    Mirrors what git-sync would have checked out: the file lives at
    /git/repo/<file_path> inside the container.
    """
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / "providers.ini").write_text(SAMPLE_INI)
    return ops.testing.Container(
        name="git-sync",
        can_connect=True,
        mounts={"content": ops.testing.Mount(location="/git/repo", source=repo_dir)},
    )


@pytest.fixture
def pat_secret():
    return ops.testing.Secret({PAT_SECRET_KEY: "custom-personal-access-token"})


def _public_relation():
    """A git relation for a public repo (no auth)."""
    return ops.testing.Relation(
        GIT_RELATION,
        interface="git",
        remote_app_data={
            "repository-url": "https://github.com/example/provider-config",
            "tracking-ref": "main",
        },
    )


def _credentials_relation(pat_secret):
    """A git relation using HTTPS credentials (username + PAT via secret)."""
    return ops.testing.Relation(
        GIT_RELATION,
        interface="git",
        remote_app_data={
            "repository-url": "https://github.com/example/private",
            "tracking-ref": "main",
            "authentication-method": git.AuthenticationMethodEnum.CREDENTIALS.value,
            "credentials-username": "git-user",
            "secret-credentials-personal-access-token": pat_secret.id,
        },
    )


def _ssh_relation():
    """A git relation using SSH auth (not supported yet)."""
    return ops.testing.Relation(
        GIT_RELATION,
        interface="git",
        remote_app_data={
            "repository-url": "git@github.com:example/private.git",
            "authentication-method": git.AuthenticationMethodEnum.SSH.value,
        },
    )


def _provider_relation():
    """The provides relation to a coordinator, so publishing has a target."""
    return ops.testing.Relation(PROVIDER_RELATION, interface="airflow_provider_configuration")


class TestReconcile:
    def test_blocked_without_file_path(self, context, container):
        """No file_path config -> BlockedStatus."""
        state = ops.testing.State(leader=True, containers=[container])
        state_out = context.run(context.on.config_changed(), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)
        assert FILE_PATH_CONFIG in state_out.unit_status.message

    def test_blocked_without_git_relation(self, context, container):
        """file_path set but no git relation -> BlockedStatus."""
        state = ops.testing.State(
            leader=True,
            containers=[container],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.config_changed(), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)

    def test_waiting_when_container_not_ready(self, context):
        """All set but container unreachable -> WaitingStatus."""
        relation = _public_relation()
        not_ready = ops.testing.Container(name="git-sync", can_connect=False)
        state = ops.testing.State(
            leader=True,
            containers=[not_ready],
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        assert isinstance(state_out.unit_status, ops.WaitingStatus)

    def test_blocked_when_file_missing(self, context, container):
        """Container ready but the configured file isn't synced -> BlockedStatus."""
        relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[container],  # no mounted file
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)
        assert "not found" in state_out.unit_status.message

    def test_active_layer_applied_and_config_published(self, context, synced_container):
        """All prerequisites met + file synced -> Active, layer set, config published."""
        git_relation = _public_relation()
        provider_relation = _provider_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, provider_relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert state_out.unit_status == ops.ActiveStatus()

        # git-sync layer is configured with the exechook.
        out_container = state_out.get_container("git-sync")
        command = out_container.layers["git-sync"].services["git-sync"].command
        assert "--repo=https://github.com/example/provider-config" in command
        assert "--exechook-command=" in command

        # The synced config was published to the provider relation.
        out_provider = state_out.get_relation(provider_relation.id)
        assert "provider-configuration" in out_provider.local_app_data

    def test_https_auth_sets_username_and_password_env(
        self, context, synced_container, pat_secret
    ):
        """HTTPS credentials -> username on CLI, token in GITSYNC_PASSWORD env."""
        relation = _credentials_relation(pat_secret)
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[relation, _provider_relation()],
            secrets=[pat_secret],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)

        out_container = state_out.get_container("git-sync")
        service = out_container.layers["git-sync"].services["git-sync"]
        assert "--username=git-user" in service.command
        assert service.environment.get("GITSYNC_PASSWORD") == "custom-personal-access-token"

    def test_blocked_on_ssh_auth(self, context, container):
        """SSH auth is not supported yet -> BlockedStatus."""
        relation = _ssh_relation()
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)


class TestSyncNowAction:
    def test_sync_now_publishes(self, context, synced_container):
        """The sync-now action re-reads and republishes the configuration."""
        git_relation = _public_relation()
        provider_relation = _provider_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, provider_relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.action("sync-now"), state)
        out_provider = state_out.get_relation(provider_relation.id)
        assert "provider-configuration" in out_provider.local_app_data

    def test_sync_now_fails_when_file_missing(self, context, container):
        """sync-now fails cleanly (not an unhandled traceback) if the file is missing."""
        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[git_relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with pytest.raises(ops.testing.ActionFailed):
            context.run(context.on.action("sync-now"), state)


class TestSensitiveData:
    def test_sensitive_data_published_from_user_secret(self, context, synced_container):
        """A valid user secret -> its sensitive values are published."""
        user_secret = ops.testing.Secret(
            {
                SENSITIVE_CONFIG_KEY: json.dumps(
                    {"databricks": {"databricks": {"token": "dapi-xxx"}}}
                )
            }
        )
        git_relation = _public_relation()
        provider_relation = _provider_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, provider_relation],
            secrets=[user_secret],
            config={
                FILE_PATH_CONFIG: "providers.ini",
                SENSITIVE_SECRET_CONFIG: user_secret.id,
            },
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert state_out.unit_status == ops.ActiveStatus()

        # The published charm secret carries the flattened sensitive value.
        out_provider = state_out.get_relation(provider_relation.id)
        charm_secret_uri = out_provider.local_app_data["provider-configuration-secret-uri"]
        content = state_out.get_secret(id=charm_secret_uri).latest_content
        assert json.loads(content["sensitive-data"]) == {"provider__databricks__token": "dapi-xxx"}

    def test_blocked_on_duplicate_sensitive_key(self, context, synced_container):
        """Two providers setting the same section.option -> BlockedStatus (spec 3.3)."""
        user_secret = ops.testing.Secret(
            {
                SENSITIVE_CONFIG_KEY: json.dumps(
                    {
                        "provider_a": {"core": {"fernet_key": "key-a"}},
                        "provider_b": {"core": {"fernet_key": "key-b"}},
                    }
                )
            }
        )
        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, _provider_relation()],
            secrets=[user_secret],
            config={
                FILE_PATH_CONFIG: "providers.ini",
                SENSITIVE_SECRET_CONFIG: user_secret.id,
            },
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)

    def test_blocked_when_sensitive_secret_not_accessible(self, context, synced_container):
        """Secret config set but the secret isn't granted/in the model -> BlockedStatus."""
        # Build a real secret to get a validly-formatted id, but do NOT add it to
        # State.secrets, so the charm cannot resolve it (simulates not-granted).
        ungranted = ops.testing.Secret({SENSITIVE_CONFIG_KEY: json.dumps({})})
        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, _provider_relation()],
            config={
                FILE_PATH_CONFIG: "providers.ini",
                SENSITIVE_SECRET_CONFIG: ungranted.id,
            },
            # note: ungranted is intentionally NOT in secrets=[...]
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)

    def test_no_sensitive_secret_publishes_non_sensitive_only(self, context, synced_container):
        """No sensitive secret config -> publishes non-sensitive config, stays Active."""
        git_relation = _public_relation()
        provider_relation = _provider_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, provider_relation],
            config={FILE_PATH_CONFIG: "providers.ini"},  # no secret config
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert state_out.unit_status == ops.ActiveStatus()

        out_provider = state_out.get_relation(provider_relation.id)
        # The charm secret holds an empty sensitive map.
        charm_secret_uri = out_provider.local_app_data["provider-configuration-secret-uri"]
        content = state_out.get_secret(id=charm_secret_uri).latest_content
        assert json.loads(content["sensitive-data"]) == {}
