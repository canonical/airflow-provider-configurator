# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the Airflow Provider Configurator charm (git input path)."""

import charms.git_integrator.v0.git as git
import ops
import ops.testing
import pytest

from charm import AirflowProviderConfiguratorCharm

GIT_RELATION = "remote-airflow-provider-configurations"
FILE_PATH_CONFIG = "airflow_provider_configurations_file_path"

# The secret content key git-integrator uses for the PAT.
PAT_SECRET_KEY = "credentials-personal-access-token"


@pytest.fixture
def context():
    return ops.testing.Context(charm_type=AirflowProviderConfiguratorCharm)


@pytest.fixture
def container():
    return ops.testing.Container(name="git-sync", can_connect=True)


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

    def test_active_and_layer_applied(self, context, container):
        """All prerequisites met -> ActiveStatus and git-sync layer present."""
        relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        assert state_out.unit_status == ops.ActiveStatus()

        out_container = state_out.get_container("git-sync")
        command = out_container.layers["git-sync"].services["git-sync"].command
        assert "--repo=https://github.com/example/provider-config" in command
        assert "--period=" in command
        assert "--ref=main" in command

    def test_https_auth_sets_username_and_password_env(self, context, container, pat_secret):
        """HTTPS credentials -> username on CLI, token in GITSYNC_PASSWORD env."""
        relation = _credentials_relation(pat_secret)
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[relation],
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
