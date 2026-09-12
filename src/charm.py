#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Provider Configurator charm application.

This charm lets Charmed Airflow operators configure Airflow providers without
manually editing airflow.cfg. Non-sensitive provider configuration is synced from
a git repository (via the git-integrator charm) into a workload container running
git-sync; sensitive configuration is supplied via a Juju user secret. Validated
configuration is relayed to the Airflow Coordinator charm over the
`airflow_provider_configuration` relation.

This module wires the git input path: the git relation, the config options, and
the git-sync workload layer. The sync-to-publish flow (Pebble notice handling,
file discovery, validation, and publishing) is added in follow-up work.
"""

import logging
import shlex
from typing import Any

import charms.git_integrator.v0.git as git
import ops

from constants import (
    CONFIG_FILE_PATH,
    CONFIG_SYNC_PERIOD,
    GIT_RELATION_NAME,
    GIT_SYNC_DEST,
    GIT_SYNC_PASSWORD_FILE,
    GIT_SYNC_ROOT,
    GIT_SYNC_SERVICE,
    INVALID_GIT_RELATION_MESSAGE,
    MISSING_FILE_PATH_MESSAGE,
    MISSING_GIT_RELATION_MESSAGE,
    SSH_NOT_SUPPORTED_MESSAGE,
    WAITING_FOR_CONTAINER_MESSAGE,
    WORKLOAD_CONTAINER,
)

logger = logging.getLogger(__name__)


class ExceptionWithStatusError(Exception):
    """Base class of exceptions for when a method has an opinion on the unit status."""

    def __init__(self, message: str, status_type):
        super().__init__(str(message))
        self.message = str(message)
        self.status_type = status_type

    @property
    def status(self) -> ops.StatusBase:
        """Return an instance of self.status_type carrying the message."""
        return self.status_type(self.message)


class AirflowProviderConfiguratorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self.git_requirer = git.GitRequires(
            self,
            GIT_RELATION_NAME,
            callback=self._reconcile,
        )
        self._container = self.unit.get_container(WORKLOAD_CONTAINER)
        self._sync_period = str(self.config[CONFIG_SYNC_PERIOD])

        # GitRequires(callback=...) already observes git_connection_information_updated,
        # relation_joined and relation_broken, so those are not repeated here.
        for event in (
            self.on.install,
            self.on.config_changed,
            self.on.update_status,
            self.on.upgrade_charm,
            self.on[WORKLOAD_CONTAINER].pebble_ready,
            self.on[GIT_RELATION_NAME].relation_changed,
        ):
            self.framework.observe(event, self._reconcile)

    # ---- config accessors -------------------------------------------------

    @property
    def _file_path(self) -> str | None:
        """The configured path to the provider .ini file, or None if unset."""
        value = self.config.get(CONFIG_FILE_PATH)
        return str(value) if value else None

    def _git_connection_info(self) -> git.GitProviderModel | None:
        """Return the git connection info for this charm's git relation, if ready.

        The relation is capped at one endpoint, so we look up this charm's single
        relation deterministically rather than iterating over all relations.
        """
        relation = self.model.get_relation(GIT_RELATION_NAME)
        if relation is None:
            return None
        return self.git_requirer.get_git_connection_information_for_relation(relation.id)

    # ---- reconcile --------------------------------------------------------

    def _reconcile(self, _: ops.EventBase) -> None:
        """Idempotent reconcile: validate prerequisites and configure git-sync."""
        try:
            self._validate_prerequisites()
        except ExceptionWithStatusError as e:
            logger.error(e)
            self._stop_git_sync()
            self.unit.status = e.status
            return
        self._configure_pebble_layer()
        self.unit.status = ops.ActiveStatus()

    def _validate_prerequisites(self) -> None:
        """Check required config, the git relation, and the container are ready.

        Raises:
            ExceptionWithStatusError: if any prerequisite is not met.
        """
        if not self._file_path:
            raise ExceptionWithStatusError(MISSING_FILE_PATH_MESSAGE, ops.BlockedStatus)
        if self.model.get_relation(GIT_RELATION_NAME) is None:
            raise ExceptionWithStatusError(MISSING_GIT_RELATION_MESSAGE, ops.BlockedStatus)
        git_info = self._git_connection_info()
        if git_info is None:
            raise ExceptionWithStatusError(INVALID_GIT_RELATION_MESSAGE, ops.BlockedStatus)
        if git_info.authentication_method == git.AuthenticationMethodEnum.SSH:
            raise ExceptionWithStatusError(SSH_NOT_SUPPORTED_MESSAGE, ops.BlockedStatus)
        if not self._container.can_connect():
            raise ExceptionWithStatusError(WAITING_FOR_CONTAINER_MESSAGE, ops.WaitingStatus)

    def _configure_pebble_layer(self) -> None:
        """Push git credentials (if any) and (re)configure the git-sync Pebble layer."""
        self._push_git_credentials()
        self._container.add_layer(GIT_SYNC_SERVICE, self._git_sync_layer, combine=True)
        self._container.replan()

    def _stop_git_sync(self) -> None:
        """Stop the git-sync service so it doesn't keep polling a stale/unrelated repo.

        Called when prerequisites are no longer met (e.g. the git relation was
        removed). Safe to call when the container is unreachable or the service
        was never started.
        """
        if not self._container.can_connect():
            return
        if GIT_SYNC_SERVICE in self._container.get_services():
            self._container.stop(GIT_SYNC_SERVICE)

    # ---- git-sync layer ---------------------------------------------------

    def _push_git_credentials(self) -> None:
        """Write the git PAT to a private file read by git-sync.

        The token is stored in a root-only file inside the container and passed to
        git-sync via GITSYNC_PASSWORD_FILE, so it appears neither on the command
        line nor in the service environment.
        """
        git_info = self._git_connection_info()
        token = git_info.credentials_personal_access_token if git_info else None
        if token:
            self._container.push(
                GIT_SYNC_PASSWORD_FILE, token, make_dirs=True, permissions=0o400
            )

    @property
    def _git_sync_layer(self) -> ops.pebble.LayerDict:
        """Build the Pebble layer that runs git-sync in continuous poll mode."""
        git_info = self._git_connection_info()
        service: dict[str, Any] = {
            "override": "replace",
            "summary": "git-sync",
            "command": self._git_sync_command(git_info),
            "startup": "enabled",
        }
        environment = self._git_sync_environment(git_info)
        if environment:
            service["environment"] = environment
        return {
            "summary": "git-sync layer",
            "description": "Continuously sync provider configuration from git.",
            "services": {GIT_SYNC_SERVICE: service},
        }

    def _git_sync_command(self, git_info: git.GitProviderModel | None) -> str:
        """Construct the git-sync command line for continuous polling.

        Runs git-sync as a long-lived service that re-syncs every `--period`.
        For HTTPS auth only the username is passed here; the token is supplied via
        a password file (see _git_sync_environment) so it never appears on the
        command line / process list. Arguments are joined with shlex.join so a
        relation value containing whitespace cannot inject extra flags.
        """
        assert git_info is not None  # guaranteed by _validate_prerequisites
        parts = [
            "/bin/git-sync",
            f"--repo={git_info.repository_url}",
            f"--root={GIT_SYNC_ROOT}",
            f"--link={GIT_SYNC_DEST}",
            f"--period={self._sync_period}",
        ]
        if git_info.tracking_ref:
            parts.append(f"--ref={git_info.tracking_ref}")
        if git_info.credentials_username:
            parts.append(f"--username={git_info.credentials_username}")
        return shlex.join(parts)

    def _git_sync_environment(self, git_info: git.GitProviderModel | None) -> dict[str, str]:
        """Environment for the git-sync service.

        The personal access token is referenced via GITSYNC_PASSWORD_FILE (a
        root-only file) rather than an inline value, so it is not exposed in the
        process list or the service environment.
        """
        env: dict[str, str] = {}
        if git_info and git_info.credentials_personal_access_token:
            env["GITSYNC_PASSWORD_FILE"] = GIT_SYNC_PASSWORD_FILE
        return env


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowProviderConfiguratorCharm)
