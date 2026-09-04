#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Provider Configurator charm application.

This charm lets Charmed Airflow operators configure Airflow providers without
manually editing airflow.cfg. Non-sensitive provider configuration is synced from
a git repository (via the git-integrator charm) into a workload container running
git-sync; validated configuration is relayed to the Airflow Coordinator charm over
the `airflow_provider_configuration` relation.

git-sync runs continuously on its `--period` timer. On each successful sync whose
content changed, it runs an `--exechook-command` script that calls `pebble notify`,
which Juju surfaces as a Pebble custom-notice event. The charm observes that event,
reads the synced .ini, and publishes the configuration.
"""

import logging

import charms.git_integrator.v0.git as git
import ops
from airflow_provider_configurator import AirflowProviderConfiguratorProvides

import config_generator

logger = logging.getLogger(__name__)

GIT_RELATION_NAME = "remote-airflow-provider-configurations"
WORKLOAD_CONTAINER = "git-sync"
GIT_SYNC_ROOT = "/git"
GIT_SYNC_DEST = "repo"  # subdir under root that git-sync checks out into
EXECHOOK_SCRIPT_PATH = "/usr/local/bin/notify-content-synced"
# Pebble custom-notice key fired by the exechook when content changes.
CONTENT_SYNCED_NOTICE_KEY = "canonical.com/airflow-provider-configurator/content-synced"

CONFIG_FILE_PATH = "airflow_provider_configurations_file_path"
CONFIG_SYNC_PERIOD = "airflow_provider_configurations_sync_period"


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
        self._config_provider = AirflowProviderConfiguratorProvides(self)

        for event in (
            self.on.install,
            self.on.config_changed,
            self.on.update_status,
            self.on.upgrade_charm,
            self.on[WORKLOAD_CONTAINER].pebble_ready,
            self.on[WORKLOAD_CONTAINER].pebble_custom_notice,
            self.on[GIT_RELATION_NAME].relation_changed,
            self.on[GIT_RELATION_NAME].relation_broken,
            self.git_requirer.on.git_connection_information_updated,
        ):
            self.framework.observe(event, self._reconcile)

        self.framework.observe(self.on.sync_now_action, self._on_sync_now_action)

    # ---- config accessors -------------------------------------------------

    @property
    def _file_path(self) -> str | None:
        """The configured path to the provider .ini file, or None if unset."""
        value = self.config.get(CONFIG_FILE_PATH)
        return str(value) if value else None

    @property
    def _sync_period(self) -> str:
        """The git-sync poll interval."""
        return str(self.config.get(CONFIG_SYNC_PERIOD, "60s"))

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
        """Idempotent reconcile: configure git-sync and publish synced config."""
        try:
            git_info = self._validate_prerequisites()
            self._configure_git_sync(git_info)
            self._publish_configuration()
        except ExceptionWithStatusError as e:
            logger.error(e)
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
            raise ExceptionWithStatusError(
                f"Missing required config: {CONFIG_FILE_PATH}", ops.BlockedStatus
            )
        git_info = self._git_connection()
        if git_info is None:
            raise ExceptionWithStatusError(
                "Waiting for the git relation to provide repository information",
                ops.BlockedStatus,
            )
        if git_info.authentication_method == "ssh":
            raise ExceptionWithStatusError(
                "SSH authentication is not supported yet; use HTTPS or a public repo",
                ops.BlockedStatus,
            )
        if not self._container.can_connect():
            raise ExceptionWithStatusError("Waiting for the git-sync container", ops.WaitingStatus)
        return git_info

    def _configure_git_sync(self, git_info: git.GitProviderModel) -> None:
        """Push the exechook script and (re)configure the git-sync Pebble layer."""
        self._container.push(
            EXECHOOK_SCRIPT_PATH,
            self._exechook_script(),
            make_dirs=True,
            permissions=0o755,
        )
        self._container.add_layer("git-sync", self._git_sync_layer(git_info), combine=True)
        self._container.replan()

    def _publish_configuration(self) -> None:
        """Read the synced .ini and publish it over the relation.

        Reads the configured file from the synced content, converts it to a
        template + sensitive map, and publishes via the provider interface.
        Sensitive data from the user secret is wired in later work; for now the
        .ini is non-sensitive only.

        Raises:
            ExceptionWithStatusError: if the configured file cannot be found.
        """
        ini_content = self._read_synced_file()
        template, sensitive_data = config_generator.build_template_and_secrets(
            ini_content, sensitive_data=None
        )
        self._config_provider.set_configuration(
            provider_configuration=template,
            provider_configuration_sensitive_data=sensitive_data,
        )

    def _read_synced_file(self) -> str:
        """Read the configured .ini from the synced git content.

        Raises:
            ExceptionWithStatusError: if the file does not exist (spec 1.3).
        """
        full_path = f"{GIT_SYNC_ROOT}/{GIT_SYNC_DEST}/{self._file_path}"
        try:
            return self._container.pull(full_path, encoding="utf-8").read()
        except ops.pebble.PathError as e:
            raise ExceptionWithStatusError(
                f"Configuration file not found at {self._file_path}; "
                "check the repository and file path.",
                ops.BlockedStatus,
            ) from e

    # ---- git-sync layer ---------------------------------------------------

    def _exechook_script(self) -> str:
        """The script git-sync runs after each changed sync; fires a Pebble notice."""
        return (
            "#!/bin/sh\n"
            "# Notify the charm that git-sync fetched new content.\n"
            f"exec pebble notify {CONTENT_SYNCED_NOTICE_KEY}\n"
        )

    def _git_sync_command(self, git_info: git.GitProviderModel) -> str:
        """Construct the git-sync command line for continuous polling.

        Runs git-sync as a long-lived service that re-syncs every `--period` and
        runs the exechook on each changed sync. For HTTPS auth only the username
        is passed here; the token is supplied via GITSYNC_PASSWORD (see
        _git_sync_environment) so it never appears on the command line.
        """
        parts = [
            "/bin/git-sync",
            f"--repo={git_info.repository_url}",
            f"--root={GIT_SYNC_ROOT}",
            f"--dest={GIT_SYNC_DEST}",
            f"--period={self._sync_period}",
            f"--exechook-command={EXECHOOK_SCRIPT_PATH}",
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

    # ---- actions ----------------------------------------------------------

    def _on_sync_now_action(self, event: ops.ActionEvent) -> None:
        """Force an immediate re-read and republish of the provider configuration."""
        try:
            self._validate_prerequisites()
            self._publish_configuration()
        except ExceptionWithStatusError as e:
            event.fail(e.message)
            return
        event.set_results({"result": "Provider configuration republished."})
        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowProviderConfiguratorCharm)
