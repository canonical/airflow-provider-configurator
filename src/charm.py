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
from typing import Any

import charms.git_integrator.v0.git as git
import ops

logger = logging.getLogger(__name__)

GIT_RELATION_NAME = "remote-airflow-provider-configurations"
WORKLOAD_CONTAINER = "git-sync"
GIT_SYNC_SERVICE = "git-sync"
GIT_SYNC_ROOT = "/git"
GIT_SYNC_DEST = "repo"  # subdir under root that git-sync checks out into

CONFIG_FILE_PATH = "airflow_provider_configurations_file_path"
CONFIG_SYNC_PERIOD = "airflow_provider_configurations_sync_period"

# Status messages (centralised so tests can assert exact equality).
MISSING_FILE_PATH_MESSAGE = f"Missing required config: {CONFIG_FILE_PATH}"
WAITING_FOR_GIT_RELATION_MESSAGE = (
    "Waiting for the git relation to provide repository information"
)
SSH_NOT_SUPPORTED_MESSAGE = (
    "SSH authentication is not supported yet; use HTTPS or a public repo"
)
WAITING_FOR_CONTAINER_MESSAGE = "Waiting for the git-sync container"


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

        for event in (
            self.on.install,
            self.on.config_changed,
            self.on.update_status,
            self.on.upgrade_charm,
            self.on[WORKLOAD_CONTAINER].pebble_ready,
            self.on[GIT_RELATION_NAME].relation_changed,
            self.on[GIT_RELATION_NAME].relation_broken,
            self.git_requirer.on.git_connection_information_updated,
        ):
            self.framework.observe(event, self._reconcile)

    # ---- config accessors -------------------------------------------------

    @property
    def _file_path(self) -> str | None:
        """The configured path to the provider .ini file, or None if unset."""
        value = self.config.get(CONFIG_FILE_PATH)
        return str(value) if value else None

    @property
    def _sync_period(self) -> str:
        """The git-sync poll interval (charmcraft.yaml guarantees a default)."""
        return str(self.config[CONFIG_SYNC_PERIOD])

    @property
    def _container(self) -> ops.Container:
        """The git-sync workload container."""
        return self.unit.get_container(WORKLOAD_CONTAINER)

    def _git_connection(self) -> git.GitProviderModel | None:
        """Return the git connection info from the relation, if ready."""
        for relation in self.git_requirer.relations:
            info = self.git_requirer.get_git_connection_information_for_relation(relation.id)
            if info:
                return info
        return None

    # ---- reconcile --------------------------------------------------------

    def _reconcile(self, _: ops.EventBase) -> None:
        """Idempotent reconcile: validate prerequisites and configure git-sync."""
        try:
            git_info = self._validate_prerequisites()
            self._configure_git_sync(git_info)
        except ExceptionWithStatusError as e:
            logger.error(e)
            self._stop_git_sync()
            self.unit.status = e.status
            return
        self.unit.status = ops.ActiveStatus()

    def _validate_prerequisites(self) -> git.GitProviderModel:
        """Check required config, the git relation, and the container are ready.

        Returns:
            The git connection info once all prerequisites are satisfied.

        Raises:
            ExceptionWithStatusError: if any prerequisite is not met.
        """
        if not self._file_path:
            raise ExceptionWithStatusError(MISSING_FILE_PATH_MESSAGE, ops.BlockedStatus)
        git_info = self._git_connection()
        if git_info is None:
            raise ExceptionWithStatusError(
                WAITING_FOR_GIT_RELATION_MESSAGE, ops.BlockedStatus
            )
        if git_info.authentication_method == git.AuthenticationMethodEnum.SSH:
            raise ExceptionWithStatusError(SSH_NOT_SUPPORTED_MESSAGE, ops.BlockedStatus)
        if not self._container.can_connect():
            raise ExceptionWithStatusError(WAITING_FOR_CONTAINER_MESSAGE, ops.WaitingStatus)
        return git_info

    def _configure_git_sync(self, git_info: git.GitProviderModel) -> None:
        """(Re)configure and start the git-sync Pebble layer."""
        self._container.add_layer(
            GIT_SYNC_SERVICE, self._git_sync_layer(git_info), combine=True
        )
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

    def _git_sync_command(self, git_info: git.GitProviderModel) -> str:
        """Construct the git-sync command line for continuous polling.

        Runs git-sync as a long-lived service that re-syncs every `--period`.
        For HTTPS auth only the username is passed here; the token is supplied via
        the GITSYNC_PASSWORD environment variable (see _git_sync_environment) so it
        never appears on the command line / process list.
        """
        parts = [
            "/bin/git-sync",
            f"--repo={git_info.repository_url}",
            f"--root={GIT_SYNC_ROOT}",
            f"--dest={GIT_SYNC_DEST}",
            f"--period={self._sync_period}",
        ]
        if git_info.tracking_ref:
            parts.append(f"--ref={git_info.tracking_ref}")
        if git_info.credentials_username:
            parts.append(f"--username={git_info.credentials_username}")
        return " ".join(parts)

    def _git_sync_environment(self, git_info: git.GitProviderModel) -> dict[str, str]:
        """Environment for the git-sync service.

        The personal access token is passed via GITSYNC_PASSWORD rather than a CLI
        flag so it is not exposed in the process list.
        """
        env: dict[str, str] = {}
        if git_info.credentials_personal_access_token:
            env["GITSYNC_PASSWORD"] = git_info.credentials_personal_access_token
        return env

    def _git_sync_layer(self, git_info: git.GitProviderModel) -> ops.pebble.LayerDict:
        """Build the Pebble layer that runs git-sync in continuous poll mode."""
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


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowProviderConfiguratorCharm)
