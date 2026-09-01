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

This module currently wires the git input path: the git relation, the config
options, and the git-sync workload layer. The sync-to-publish flow (Pebble notice
handling, file discovery, validation, and publishing) is added in follow-up work.
"""

import logging

import charms.git_integrator.v0.git as git
import ops

logger = logging.getLogger(__name__)

GIT_RELATION_NAME = "remote-airflow-provider-configurations"
WORKLOAD_CONTAINER = "git-sync"
GIT_SYNC_ROOT = "/git"
GIT_SYNC_DEST = "repo"  # subdir under root that git-sync checks out into

CONFIG_FILE_PATH = "airflow_provider_configurations_file_path"
CONFIG_SYNC_PERIOD = "airflow_provider_configurations_sync_period"


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

    @property
    def _file_path(self) -> str | None:
        """The configured path to the provider .ini file, or None if unset."""
        value = self.config.get(CONFIG_FILE_PATH)
        return str(value) if value else None

    @property
    def _sync_period(self) -> str:
        """The git-sync poll interval."""
        return str(self.config.get(CONFIG_SYNC_PERIOD, "60s"))

    def _git_connection(self) -> git.GitProviderModel | None:
        """Return the git connection info from the relation, if ready."""
        for relation in self.git_requirer.relations:
            info = self.git_requirer.get_git_connection_information_for_relation(relation.id)
            if info:
                return info
        return None

    def _reconcile(self, _: ops.EventBase) -> None:
        """Bring the charm to its desired state.

        Validates required config and the git relation, then (re)configures the
        git-sync workload layer. The sync-to-publish flow is handled separately.
        """
        # Required configuration: the .ini path.
        if not self._file_path:
            self.unit.status = ops.BlockedStatus(f"Missing required config: {CONFIG_FILE_PATH}")
            return

        # The git relation must be present and ready.
        git_info = self._git_connection()
        if git_info is None:
            self.unit.status = ops.BlockedStatus(
                "Waiting for the git relation to provide repository information"
            )
            return

        # SSH authentication is not supported yet.
        if git_info.authentication_method == "ssh":
            self.unit.status = ops.BlockedStatus(
                "SSH authentication is not supported yet; use HTTPS or a public repo"
            )
            return

        # The workload container must be reachable.
        container = self.unit.get_container(WORKLOAD_CONTAINER)
        if not container.can_connect():
            self.unit.status = ops.WaitingStatus("Waiting for the git-sync container")
            return

        container.add_layer("git-sync", self._git_sync_layer(git_info), combine=True)
        container.replan()

        self.unit.status = ops.ActiveStatus()

    def _git_sync_command(self, git_info: git.GitProviderModel) -> str:
        """Construct the git-sync command line for continuous polling.

        Uses the flag set from the git-sync image (see
        https://github.com/kubernetes/git-sync). Unlike a one-shot
        (`--one-time`) invocation, this runs git-sync as a long-lived service
        that re-syncs every `--period`. For HTTPS auth, only the username is
        passed here; the token is supplied via the GITSYNC_PASSWORD environment
        variable (see _git_sync_environment) so it never appears on the command
        line.
        """
        parts = [
            "/bin/git-sync",
            f"--repo={git_info.repository_url}",
            f"--root={GIT_SYNC_ROOT}",
            f"--dest={GIT_SYNC_DEST}",
            f"--period={self._sync_period}",
        ]
        if git_info.tracking_ref:
            # tracking_ref may be a branch or a revision; git-sync accepts --ref.
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
        service: dict = {
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
            "services": {"git-sync": service},
        }


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowProviderConfiguratorCharm)
