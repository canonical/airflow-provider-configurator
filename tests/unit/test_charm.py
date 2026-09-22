# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the Airflow Provider Configurator charm (git input path)."""

import shlex

import charms.git_integrator.v0.git as git
import ops
import ops.testing
import pytest

import charm as charm_module
from charm import AirflowProviderConfiguratorCharm

GIT_RELATION = "remote-airflow-provider-configurations"
FILE_PATH_CONFIG = "airflow_provider_configurations_file_path"
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
        state = ops.testing.State(leader=True, containers=[container])
        state_out = context.run(context.on.config_changed(), state)
        assert state_out.unit_status == ops.BlockedStatus(charm_module.MISSING_FILE_PATH_MESSAGE)

    def test_blocked_without_git_relation(self, context, container):
        state = ops.testing.State(
            leader=True,
            containers=[container],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.config_changed(), state)
        assert state_out.unit_status == ops.BlockedStatus(
            charm_module.MISSING_GIT_RELATION_MESSAGE
        )

    def test_waiting_when_container_not_ready(self, context):
        relation = _public_relation()
        not_ready = ops.testing.Container(name="git-sync", can_connect=False)
        state = ops.testing.State(
            leader=True,
            containers=[not_ready],
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        assert state_out.unit_status == ops.WaitingStatus(
            charm_module.WAITING_FOR_CONTAINER_MESSAGE
        )

    def test_active_and_layer_applied(self, context, container):
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
        service = out_container.layers["git-sync"].services["git-sync"]
        assert "--repo=https://github.com/example/provider-config" in service.command
        assert "--period=" in service.command
        assert "--ref=main" in service.command
        # Public repo: no password material leaked to the command or environment.
        assert "GITSYNC_PASSWORD" not in (service.environment or {})
        assert "GITSYNC_PASSWORD_FILE" not in (service.environment or {})

    def test_https_auth_sets_username_and_writes_password_file(
        self, context, container, pat_secret
    ):
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

        # Username is on the command line; the token is not.
        assert "--username=git-user" in service.command
        assert "custom-personal-access-token" not in service.command

        # The token is referenced via a private file, not an inline env value.
        assert (
            service.environment.get("GITSYNC_PASSWORD_FILE") == charm_module.GIT_SYNC_PASSWORD_FILE
        )
        assert "GITSYNC_PASSWORD" not in service.environment

        # And the token was actually written to that file inside the container.
        container_root = out_container.get_filesystem(context)
        password_file = container_root / charm_module.GIT_SYNC_PASSWORD_FILE.lstrip("/")
        assert password_file.read_text() == "custom-personal-access-token"

    def test_command_is_injection_safe(self, context, container):
        """A relation value containing whitespace must not inject extra git-sync flags."""
        relation = ops.testing.Relation(
            GIT_RELATION,
            interface="git",
            remote_app_data={
                "repository-url": "https://github.com/example/provider-config",
                "tracking-ref": "main --exechook-command=/bin/sh",
            },
        )
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        service = state_out.get_container("git-sync").layers["git-sync"].services["git-sync"]
        # shlex.join quotes the value so the injected flag stays part of --ref.
        assert "--exechook-command" not in shlex.split(service.command)

    def test_blocked_on_ssh_auth(self, context, container):
        relation = _ssh_relation()
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        assert state_out.unit_status == ops.BlockedStatus(charm_module.SSH_NOT_SUPPORTED_MESSAGE)

    def test_blocked_on_invalid_git_relation(self, context, container):
        """A git relation present but without usable connection info blocks the unit."""
        relation = ops.testing.Relation(GIT_RELATION, interface="git", remote_app_data={})
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)
        assert state_out.unit_status == ops.BlockedStatus(
            charm_module.INVALID_GIT_RELATION_MESSAGE
        )

    def test_blocked_when_credentials_push_fails(
        self, context, container, pat_secret, monkeypatch
    ):
        """If writing the credentials file fails, the unit blocks instead of crashing."""
        relation = _credentials_relation(pat_secret)
        state = ops.testing.State(
            leader=True,
            containers=[container],
            relations=[relation],
            secrets=[pat_secret],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )

        def _raise_path_error(*args, **kwargs):
            raise ops.pebble.PathError("generic-file-error", "cannot write file")

        monkeypatch.setattr(ops.Container, "push", _raise_path_error)
        state_out = context.run(context.on.relation_changed(relation), state)
        assert state_out.unit_status == ops.BlockedStatus(
            "Failed to write git credentials to the workload container."
        )

    def test_git_sync_stopped_when_relation_broken(self, context, container):
        """When the git relation is removed, git-sync must stop (no stale polling)."""
        # Start with a running git-sync service in the container.
        running = ops.testing.Container(
            name="git-sync",
            can_connect=True,
            layers={
                "git-sync": ops.pebble.Layer(
                    {
                        "services": {
                            "git-sync": {
                                "override": "replace",
                                "command": "/bin/git-sync --repo=x",
                                "startup": "enabled",
                            }
                        }
                    }
                )
            },
            service_statuses={"git-sync": ops.pebble.ServiceStatus.ACTIVE},
        )
        # No git relation present -> prerequisites fail -> git-sync should be stopped.
        state = ops.testing.State(
            leader=True,
            containers=[running],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.update_status(), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)
        out_container = state_out.get_container("git-sync")
        assert out_container.service_statuses.get("git-sync") != ops.pebble.ServiceStatus.ACTIVE

    def test_git_credentials_removed_when_relation_broken(self, context, tmp_path):
        """Removing the git relation must also delete any stored credentials file."""
        # Pre-seed a credentials file inside the container via a mount.
        creds_dir = tmp_path / "git-creds"
        creds_dir.mkdir()
        password_file = creds_dir / "password"
        password_file.write_text("stale-token")
        mount_point = charm_module.GIT_SYNC_PASSWORD_FILE.rsplit("/", 1)[0]
        seeded = ops.testing.Container(
            name="git-sync",
            can_connect=True,
            mounts={"creds": ops.testing.Mount(location=mount_point, source=creds_dir)},
        )
        # No git relation present -> prerequisites fail -> credentials removed.
        state = ops.testing.State(
            leader=True,
            containers=[seeded],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        context.run(context.on.update_status(), state)
        assert not password_file.exists()
