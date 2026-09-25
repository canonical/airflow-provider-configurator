# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the Airflow Provider Configurator charm (git input path)."""

import json
import shlex

import charms.git_integrator.v0.git as git
import ops
import ops.testing
import pytest

import charm as charm_module
from charm import AirflowProviderConfiguratorCharm

GIT_RELATION = "remote-airflow-provider-configurations"
PROVIDER_RELATION = "airflow-provider-configuration"
FILE_PATH_CONFIG = "airflow_provider_configurations_file_path"

SENSITIVE_SECRET_CONFIG = "airflow_provider_configurations_secret"
SENSITIVE_CONFIG_KEY = "airflow-provider-configurations"

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
def subdir_synced_container(tmp_path):
    """A git-sync container whose provider .ini lives under a `path` subdirectory.

    Mirrors a repo scoped with the git relation's `path` field: the file is at
    /git/repo/<path>/<file_path> inside the container.
    """
    repo_dir = tmp_path / "repo"
    subdir = repo_dir / "custom" / "sub"
    subdir.mkdir(parents=True)
    (subdir / "providers.ini").write_text(SAMPLE_INI)
    return ops.testing.Container(
        name="git-sync",
        can_connect=True,
        mounts={"content": ops.testing.Mount(location="/git/repo", source=repo_dir)},
    )


@pytest.fixture
def pat_secret():
    return ops.testing.Secret({PAT_SECRET_KEY: "custom-personal-access-token"})


@pytest.fixture
def empty_synced_container(tmp_path):
    """A git-sync container whose synced provider .ini is present but empty.

    Models an admin deliberately committing an empty file to remove all provider
    config (spec 2.2), as distinct from a file that is absent entirely (spec 1.3).
    """
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / "providers.ini").write_text("")
    return ops.testing.Container(
        name="git-sync",
        can_connect=True,
        mounts={"content": ops.testing.Mount(location="/git/repo", source=repo_dir)},
    )


def _peer_relation(local_app_data=None):
    """The replicas peer relation used to store the published-config hash."""
    return ops.testing.PeerRelation(
        "replicas",
        interface="airflow_provider_configurator_replica",
        local_app_data=local_app_data or {},
    )


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


def _subdir_relation():
    """A git relation that scopes the config under a `path` subdirectory."""
    return ops.testing.Relation(
        GIT_RELATION,
        interface="git",
        remote_app_data={
            "repository-url": "https://github.com/example/provider-config",
            "tracking-ref": "main",
            "path": "custom/sub",
        },
    )


def _running_git_sync_container(repo_dir):
    """A container whose git-sync service is in the plan and active.

    sync-now signals the running service, so the service must exist and be
    running for the signal to be delivered.
    """
    return ops.testing.Container(
        name="git-sync",
        can_connect=True,
        mounts={"content": ops.testing.Mount(location="/git/repo", source=repo_dir)},
        layers={
            "git-sync": ops.pebble.Layer(
                {
                    "services": {
                        "git-sync": {
                            "override": "replace",
                            "command": "/bin/git-sync",
                            "startup": "enabled",
                        }
                    }
                }
            )
        },
        service_statuses={"git-sync": ops.pebble.ServiceStatus.ACTIVE},
    )


def _patch_sync_wait(monkeypatch, counts, timeout=None):
    """Simulate git-sync's sync counter while sync-now waits, without real sleeps.

    `counts` are (total, errors) pairs returned in order by _git_sync_counts (the
    last value repeats), so [(1, 0), (2, 0)] means "the sync completed" and
    [(1, 0)] means "it never did". `timeout` overrides the wait budget so the
    failure path is instant.
    """
    remaining = list(counts)

    def fake_counts(_self):
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    monkeypatch.setattr(
        charm_module.AirflowProviderConfiguratorCharm,
        "_git_sync_counts",
        fake_counts,
    )
    monkeypatch.setattr(charm_module, "SYNC_NOW_POLL_INTERVAL_SECONDS", 0)
    if timeout is not None:
        monkeypatch.setattr(charm_module, "SYNC_NOW_TIMEOUT_SECONDS", timeout)


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
        assert state_out.unit_status == ops.BlockedStatus(
            charm_module.MISSING_GIT_RELATION_MESSAGE
        )

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
        service = out_container.layers["git-sync"].services["git-sync"]
        assert "--repo=https://github.com/example/provider-config" in service.command
        assert "--period=" in service.command
        assert "--ref=main" in service.command
        assert "--exechook-command=" in service.command
        # Public repo: no password material leaked to the command or environment.
        assert "GITSYNC_PASSWORD" not in (service.environment or {})
        assert "GITSYNC_PASSWORD_FILE" not in (service.environment or {})

        # The synced config was published to the provider relation.
        out_provider = state_out.get_relation(provider_relation.id)
        assert "provider-configuration" in out_provider.local_app_data

    def test_config_read_from_relation_path_subdirectory(self, context, subdir_synced_container):
        """The git relation's `path` scopes where the config file is read from."""
        git_relation = _subdir_relation()
        provider_relation = _provider_relation()
        state = ops.testing.State(
            leader=True,
            containers=[subdir_synced_container],
            relations=[git_relation, provider_relation],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert state_out.unit_status == ops.ActiveStatus()
        out_provider = state_out.get_relation(provider_relation.id)
        assert "provider-configuration" in out_provider.local_app_data

    def test_blocked_when_file_missing_under_relation_path(self, context, synced_container):
        """`path` is set but the file isn't under that subdirectory -> BlockedStatus."""
        git_relation = _subdir_relation()  # expects /git/repo/custom/sub/providers.ini
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],  # file is at /git/repo/providers.ini, not the subdir
            relations=[git_relation, _provider_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)
        assert "custom/sub/providers.ini" in state_out.unit_status.message

    def test_https_auth_sets_username_and_writes_password_file(
        self, context, synced_container, pat_secret
    ):
        """HTTPS credentials -> username on CLI, token written to a private file."""
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

    def test_token_without_username_does_not_configure_auth(
        self, context, synced_container, pat_secret
    ):
        """A token with no username must not reach git-sync at all.

        git-sync exits immediately on `--password-file` without `--username`, so
        a layer built from half a credential pair puts the unit in error. The
        integrator library produces exactly that shape whenever the relation's
        authentication method is unset: it leaves the token populated (as the
        placeholder "None") while never supplying a username.
        """
        relation = ops.testing.Relation(
            GIT_RELATION,
            interface="git",
            remote_app_data={
                "repository-url": "https://github.com/example/provider-config",
                "tracking-ref": "main",
                "secret-credentials-personal-access-token": pat_secret.id,
            },
        )
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[relation, _provider_relation()],
            secrets=[pat_secret],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(relation), state)

        assert state_out.unit_status == ops.ActiveStatus()
        out_container = state_out.get_container("git-sync")
        service = out_container.layers["git-sync"].services["git-sync"]
        assert "--username" not in service.command
        assert "GITSYNC_PASSWORD_FILE" not in (service.environment or {})

        # No half-usable credential is left behind in the container either.
        container_root = out_container.get_filesystem(context)
        password_file = container_root / charm_module.GIT_SYNC_PASSWORD_FILE.lstrip("/")
        assert not password_file.exists()

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

    def test_git_sync_stopped_when_relation_broken(self, context):
        """When the git relation is removed, git-sync must stop (no stale polling)."""
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


class TestSyncNowAction:
    def test_sync_now_publishes(self, context, tmp_path, monkeypatch):
        """The sync-now action signals git-sync, waits, then republishes."""
        git_relation = _public_relation()
        provider_relation = _provider_relation()
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        (repo_dir / "providers.ini").write_text(SAMPLE_INI)
        _patch_sync_wait(monkeypatch, counts=[(1.0, 0.0), (2.0, 0.0)])
        state = ops.testing.State(
            leader=True,
            containers=[_running_git_sync_container(repo_dir)],
            relations=[git_relation, provider_relation, _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.action("sync-now"), state)
        out_provider = state_out.get_relation(provider_relation.id)
        assert "provider-configuration" in out_provider.local_app_data

    def test_sync_now_fails_when_service_not_running(self, context, tmp_path):
        """sync-now fails cleanly when git-sync is not running to be signalled."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        (repo_dir / "providers.ini").write_text(SAMPLE_INI)
        # No git-sync layer in the plan -> send_signal raises -> action fails
        # rather than falsely reporting a republish.
        not_running = ops.testing.Container(
            name="git-sync",
            can_connect=True,
            mounts={"content": ops.testing.Mount(location="/git/repo", source=repo_dir)},
        )
        state = ops.testing.State(
            leader=True,
            containers=[not_running],
            relations=[_public_relation(), _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with pytest.raises(ops.testing.ActionFailed):
            context.run(context.on.action("sync-now"), state)

    def test_sync_now_fails_when_sync_does_not_complete(
        self, context, tmp_path, monkeypatch
    ):
        """sync-now fails if the sync counter never advances within the timeout."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        (repo_dir / "providers.ini").write_text(SAMPLE_INI)
        # Counter never advances -> the wait loop expires -> action fails.
        _patch_sync_wait(monkeypatch, counts=[(1.0, 0.0)], timeout=0)
        state = ops.testing.State(
            leader=True,
            containers=[_running_git_sync_container(repo_dir)],
            relations=[_public_relation(), _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with pytest.raises(ops.testing.ActionFailed):
            context.run(context.on.action("sync-now"), state)

    def test_sync_now_fails_when_fetch_errors(self, context, tmp_path, monkeypatch):
        """sync-now fails when the sync it triggered errored, not just on timeout."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        (repo_dir / "providers.ini").write_text(SAMPLE_INI)
        # The counter advances, but the increment is in the error bucket.
        _patch_sync_wait(monkeypatch, counts=[(1.0, 0.0), (2.0, 1.0)])
        state = ops.testing.State(
            leader=True,
            containers=[_running_git_sync_container(repo_dir)],
            relations=[_public_relation(), _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with pytest.raises(ops.testing.ActionFailed) as excinfo:
            context.run(context.on.action("sync-now"), state)
        assert excinfo.value.message == charm_module.SYNC_NOW_FETCH_FAILED_MESSAGE

    def test_sync_now_fails_when_git_sync_restarts(self, context, tmp_path, monkeypatch):
        """A counter reset means git-sync died mid-sync, which is a failure."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        (repo_dir / "providers.ini").write_text(SAMPLE_INI)
        # git-sync exits after a failed sync, so its counters restart from zero.
        _patch_sync_wait(monkeypatch, counts=[(5.0, 0.0), (1.0, 0.0)])
        state = ops.testing.State(
            leader=True,
            containers=[_running_git_sync_container(repo_dir)],
            relations=[_public_relation(), _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with pytest.raises(ops.testing.ActionFailed) as excinfo:
            context.run(context.on.action("sync-now"), state)
        assert excinfo.value.message == charm_module.SYNC_NOW_FETCH_FAILED_MESSAGE

    def test_sync_now_fails_when_prerequisites_unmet(self, context, container):
        """sync-now fails cleanly (not a traceback) when prerequisites are unmet."""
        # No git relation -> _validate_prerequisites raises before any fetch.
        state = ops.testing.State(
            leader=True,
            containers=[container],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with pytest.raises(ops.testing.ActionFailed):
            context.run(context.on.action("sync-now"), state)


class TestGitSyncCounts:
    """Parsing of git-sync's Prometheus counter, which signals sync completion."""

    @staticmethod
    def _patch_endpoint(monkeypatch, *, status=200, payload="", error=None):
        """Stand in for git-sync's metrics endpoint."""

        class FakeResponse:
            def __init__(self):
                self.status = status

            def read(self):
                return payload.encode()

        class FakeConnection:
            def __init__(self, *args, **kwargs):
                pass

            def request(self, *args, **kwargs):
                if error is not None:
                    raise error

            def getresponse(self):
                return FakeResponse()

            def close(self):
                pass

        monkeypatch.setattr(charm_module.http.client, "HTTPConnection", FakeConnection)

    def test_unchanged_sync_counts_towards_total(self, context, container, monkeypatch):
        """A 'noop' sync must be counted: it is how an unchanged repo reports success.

        Regression test for using --touch-file as the completion signal. git-sync
        only touches that file when the content changed, so sync-now against an
        unchanged repository waited for the full timeout and falsely reported
        failure.
        """
        self._patch_endpoint(
            monkeypatch,
            payload=(
                "# HELP git_sync_count_total How many git syncs completed\n"
                "# TYPE git_sync_count_total counter\n"
                'git_sync_count_total{status="success"} 2\n'
                'git_sync_count_total{status="noop"} 3\n'
            ),
        )
        with context(context.on.update_status(), ops.testing.State(containers=[container])) as mgr:
            assert mgr.charm._git_sync_counts() == (5.0, 0.0)

    def test_errors_are_reported_separately(self, context, container, monkeypatch):
        """Failed syncs are counted so sync-now can report a fetch failure."""
        self._patch_endpoint(
            monkeypatch,
            payload=(
                'git_sync_count_total{status="success"} 1\n'
                'git_sync_count_total{status="error"} 4\n'
            ),
        )
        with context(context.on.update_status(), ops.testing.State(containers=[container])) as mgr:
            assert mgr.charm._git_sync_counts() == (5.0, 4.0)

    def test_unreachable_endpoint_returns_none(self, context, container, monkeypatch):
        """An unreadable endpoint is inconclusive, not a completed sync."""
        self._patch_endpoint(monkeypatch, error=OSError("connection refused"))
        with context(context.on.update_status(), ops.testing.State(containers=[container])) as mgr:
            assert mgr.charm._git_sync_counts() is None

    def test_non_200_returns_none(self, context, container, monkeypatch):
        """Same for an endpoint that responds but not with metrics."""
        self._patch_endpoint(monkeypatch, status=503, payload="repo is not ready")
        with context(context.on.update_status(), ops.testing.State(containers=[container])) as mgr:
            assert mgr.charm._git_sync_counts() is None


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

    def test_blocked_when_sensitive_secret_missing_payload_key(self, context, synced_container):
        """Secret exists but lacks the payload key -> BlockedStatus, not silently empty."""
        # A secret with the wrong key is an operator mistake; treating it as "no
        # sensitive data" would silently drop the values they intended to set.
        wrong_key = ops.testing.Secret({"wrong-key": json.dumps({"a": {"b": {"c": "d"}}})})
        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, _provider_relation()],
            secrets=[wrong_key],
            config={
                FILE_PATH_CONFIG: "providers.ini",
                SENSITIVE_SECRET_CONFIG: wrong_key.id,
            },
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


class TestConfigHashDedup:
    """Peer-hash dedup (spec 1.2): republish only when the config changed."""

    def test_hash_stored_after_publish(self, context, synced_container):
        """A successful publish records the config hash in the peer databag."""
        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert state_out.unit_status == ops.ActiveStatus()

        peer = state_out.get_relation(
            next(r.id for r in state_out.relations if r.endpoint == "replicas")
        )
        assert peer.local_app_data.get(charm_module.PEER_CONFIG_HASH_KEY)

    def test_unchanged_config_skips_republish(self, context, synced_container):
        """A second reconcile with identical content must not rewrite the secret."""
        from unittest.mock import patch

        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        # First reconcile publishes and stores the hash.
        state_after_first = context.run(context.on.relation_changed(git_relation), state)

        # Second reconcile over the resulting state (hash already stored): the
        # publish path must be skipped, so set_configuration is never called.
        with patch("charm.AirflowProviderConfiguratorProvides.set_configuration") as mock_set:
            context.run(context.on.update_status(), state_after_first)
            mock_set.assert_not_called()

    def test_changed_config_republishes(self, context, synced_container, tmp_path):
        """When the synced content changes, the new config is republished."""
        from unittest.mock import patch

        git_relation = _public_relation()
        # Pre-seed the peer databag with a stale hash so any real config differs.
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[
                git_relation,
                _provider_relation(),
                _peer_relation(local_app_data={charm_module.PEER_CONFIG_HASH_KEY: "stale"}),
            ],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with patch("charm.AirflowProviderConfiguratorProvides.set_configuration") as mock_set:
            context.run(context.on.relation_changed(git_relation), state)
            mock_set.assert_called_once()

    def test_new_relation_republishes_despite_unchanged_hash(self, context, synced_container):
        """A freshly-joined relation is populated even when the content hash is unchanged.

        Content-only dedup must not strand a relation that joined (or was re-added)
        after the hash was stored: is_published() forces the publish so the new
        relation databag gets the configuration (spec 1.2).
        """
        from unittest.mock import patch

        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        # First reconcile publishes and stores the hash.
        state_after_first = context.run(context.on.relation_changed(git_relation), state)
        stored_hash = state_after_first.get_relation(
            next(r.id for r in state_after_first.relations if r.endpoint == "replicas")
        ).local_app_data[charm_module.PEER_CONFIG_HASH_KEY]

        # A brand-new provider relation joins with an empty databag while the
        # stored hash still matches the unchanged content: the publish must run.
        state_readded = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[
                git_relation,
                _provider_relation(),  # fresh relation, empty databag
                _peer_relation(local_app_data={charm_module.PEER_CONFIG_HASH_KEY: stored_hash}),
            ],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with patch("charm.AirflowProviderConfiguratorProvides.set_configuration") as mock_set:
            context.run(context.on.relation_changed(git_relation), state_readded)
            mock_set.assert_called_once()

    def test_relation_joined_publishes_to_the_new_consumer(self, context, synced_container):
        """Joining the provider relation is itself enough to get the data published.

        The dedup escape hatch above is only reachable if some hook actually runs
        when a consumer joins. Driving the consumer's own relation-joined event
        here (rather than an unrelated one) is what proves that endpoint is
        observed; otherwise a new consumer stays empty until an unrelated event
        happens to reconcile.
        """
        provider_relation = _provider_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[_public_relation(), provider_relation, _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_joined(provider_relation), state)
        out_provider = state_out.get_relation(provider_relation.id)
        assert "provider-configuration" in out_provider.local_app_data


class TestEmptyConfigCleanup:
    """Empty-data cleanup (spec 2.2): present-but-empty file clears published data."""

    def test_present_but_empty_file_is_active_and_clears(self, context, empty_synced_container):
        """An empty (but present) file -> Active + clear_configuration (spec 2.2)."""
        from unittest.mock import patch

        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[empty_synced_container],
            relations=[
                git_relation,
                _provider_relation(),
                _peer_relation(local_app_data={charm_module.PEER_CONFIG_HASH_KEY: "stale"}),
            ],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with (
            patch("charm.AirflowProviderConfiguratorProvides.clear_configuration") as mock_clear,
            patch("charm.AirflowProviderConfiguratorProvides.set_configuration") as mock_set,
        ):
            state_out = context.run(context.on.relation_changed(git_relation), state)
            mock_clear.assert_called_once()
            mock_set.assert_not_called()
        # Present-but-empty is a valid state, not a blocked one (contrast spec 1.3).
        assert state_out.unit_status == ops.ActiveStatus()

    def test_absent_file_still_blocks(self, context, container):
        """A file that is absent entirely still blocks (spec 1.3, unchanged)."""
        git_relation = _public_relation()
        state = ops.testing.State(
            leader=True,
            containers=[container],  # no mounted repo -> file absent
            relations=[git_relation, _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.relation_changed(git_relation), state)
        assert isinstance(state_out.unit_status, ops.BlockedStatus)

    def test_sensitive_only_is_valid_publish_not_cleanup(self, context, empty_synced_container):
        """Empty .ini but sensitive data present -> valid publish, not cleanup.

        This is the Q1 case: 'empty' means BOTH inputs empty. Sensitive-only is a
        legitimate configuration and must publish, not trigger cleanup.
        """
        from unittest.mock import patch

        git_relation = _public_relation()
        user_secret = ops.testing.Secret(
            {SENSITIVE_CONFIG_KEY: json.dumps({"p": {"gcs": {"conn_id": "s3cret"}}})}
        )
        state = ops.testing.State(
            leader=True,
            containers=[empty_synced_container],
            relations=[git_relation, _provider_relation(), _peer_relation()],
            secrets=[user_secret],
            config={
                FILE_PATH_CONFIG: "providers.ini",
                SENSITIVE_SECRET_CONFIG: user_secret.id,
            },
        )
        with (
            patch("charm.AirflowProviderConfiguratorProvides.clear_configuration") as mock_clear,
            patch("charm.AirflowProviderConfiguratorProvides.set_configuration") as mock_set,
        ):
            state_out = context.run(context.on.relation_changed(git_relation), state)
            mock_set.assert_called_once()
            mock_clear.assert_not_called()
        assert state_out.unit_status == ops.ActiveStatus()

    def test_cleared_state_not_recleared_when_unchanged(self, context, empty_synced_container):
        """Once cleared, an unchanged empty config must NOT re-run clear_configuration.

        Regression for the review comment: is_cleared() lets the empty state be
        deduplicated too, so a second reconcile with the same empty result skips
        the clear instead of repeating it on every event (spec 2.2).
        """
        from unittest.mock import patch

        git_relation = _public_relation()
        # First reconcile: empty file with a stale stored hash -> clears + stores hash.
        state = ops.testing.State(
            leader=True,
            containers=[empty_synced_container],
            relations=[
                git_relation,
                _provider_relation(),
                _peer_relation(local_app_data={charm_module.PEER_CONFIG_HASH_KEY: "stale"}),
            ],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_after_first = context.run(context.on.relation_changed(git_relation), state)

        # Second reconcile over the resulting state: hash now matches AND relations
        # are already cleared, so clear_configuration must not be called again.
        with patch("charm.AirflowProviderConfiguratorProvides.clear_configuration") as mock_clear:
            context.run(context.on.update_status(), state_after_first)
            mock_clear.assert_not_called()


class TestConfigSourceRemoved:
    """The published config is withdrawn when its source goes away (spec 2.2, 4.2)."""

    def test_published_config_cleared_when_git_relation_removed(self, context, synced_container):
        """Removing the git relation empties the databag so the coordinator reconfigures."""
        git_relation = _public_relation()
        provider_relation = _provider_relation()
        state = ops.testing.State(
            leader=True,
            containers=[synced_container],
            relations=[git_relation, provider_relation, _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        published = context.run(context.on.relation_changed(git_relation), state)
        out_provider = published.get_relation(provider_relation.id)
        assert "provider-configuration" in out_provider.local_app_data

        # Drop the git relation and reconcile: the config we published can no
        # longer be refreshed, so it must be withdrawn immediately.
        without_git = ops.testing.State(
            leader=True,
            containers=list(published.containers),
            relations=[r for r in published.relations if r.endpoint != GIT_RELATION],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        state_out = context.run(context.on.update_status(), without_git)
        out_provider = state_out.get_relation(provider_relation.id)
        assert "provider-configuration" not in out_provider.local_app_data
        assert "provider-configuration-secret-uri" not in out_provider.local_app_data
        # The stored hash is dropped too, so a re-added source republishes.
        out_peer = next(r for r in state_out.relations if r.endpoint == "replicas")
        assert charm_module.PEER_CONFIG_HASH_KEY not in out_peer.local_app_data

    def test_published_config_kept_when_container_not_ready(self, context):
        """A transient wait (container down) must not withdraw a valid config."""
        from unittest.mock import patch

        unreachable = ops.testing.Container(name="git-sync", can_connect=False)
        state = ops.testing.State(
            leader=True,
            containers=[unreachable],
            relations=[_public_relation(), _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        with patch("charm.AirflowProviderConfiguratorProvides.clear_configuration") as mock_clear:
            state_out = context.run(context.on.update_status(), state)
            mock_clear.assert_not_called()
        assert isinstance(state_out.unit_status, ops.WaitingStatus)


class TestSyncNowForce:
    """sync-now bypasses the content-hash dedup (review: force republish)."""

    def test_sync_now_republishes_even_when_unchanged(self, context, tmp_path, monkeypatch):
        """sync-now must publish even when the hash is unchanged (force=True)."""
        from unittest.mock import patch

        git_relation = _public_relation()
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        (repo_dir / "providers.ini").write_text(SAMPLE_INI)
        synced = _running_git_sync_container(repo_dir)
        # Pre-seed the peer hash so a normal reconcile would dedup and skip.
        state = ops.testing.State(
            leader=True,
            containers=[synced],
            relations=[git_relation, _provider_relation(), _peer_relation()],
            config={FILE_PATH_CONFIG: "providers.ini"},
        )
        # First establish the published state + stored hash.
        state = context.run(context.on.relation_changed(git_relation), state)

        # Now sync-now: even though nothing changed, it must call set_configuration.
        _patch_sync_wait(monkeypatch, counts=[(1.0, 0.0), (2.0, 0.0)])
        with patch("charm.AirflowProviderConfiguratorProvides.set_configuration") as mock_set:
            context.run(context.on.action("sync-now"), state)
            mock_set.assert_called_once()
