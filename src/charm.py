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

git-sync runs continuously on its `--period` timer. On each successful sync whose
content changed, it runs an `--exechook-command` script that calls `pebble notify`,
which Juju surfaces as a Pebble custom-notice event. The charm observes that event,
reads the synced .ini, combines it with sensitive data from the user secret, and
publishes the configuration.
"""

import json
import logging
import shlex
from typing import Any

import charms.git_integrator.v0.git as git
import ops
from airflow_provider_configurator import AirflowProviderConfiguratorProvides

import config_generator
import sensitive_config
from constants import (
    CONFIG_FILE_PATH,
    CONFIG_SENSITIVE_SECRET,
    CONFIG_SYNC_PERIOD,
    CONTENT_SYNCED_NOTICE_KEY,
    EXECHOOK_SCRIPT_PATH,
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
        self._config_provider = AirflowProviderConfiguratorProvides(self)

        # GitRequires(callback=...) already observes git_connection_information_updated,
        # relation_joined and relation_broken, so those are not repeated here.
        for event in (
            self.on.install,
            self.on.config_changed,
            self.on.update_status,
            self.on.upgrade_charm,
            self.on.secret_changed,
            self.on[WORKLOAD_CONTAINER].pebble_ready,
            self.on[WORKLOAD_CONTAINER].pebble_custom_notice,
            self.on[GIT_RELATION_NAME].relation_changed,
        ):
            self.framework.observe(event, self._reconcile)

        self.framework.observe(self.on.sync_now_action, self._on_sync_now_action)

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

    def _sensitive_data(self) -> dict[str, dict[str, str]]:
        """Return the flattened sensitive provider config from the user secret.

        The sensitive secret is optional: if the config option is unset, there is
        no sensitive data and an empty map is returned. If the option is set but
        the secret is unreadable (not granted, missing), its payload is invalid,
        or two providers collide on the same section.option, the unit blocks.

        Raises:
            ExceptionWithStatusError: if the secret is set but cannot be read or
                parsed, or if a duplicate section.option is found (spec 3.3).
        """
        secret_id = self.config.get(CONFIG_SENSITIVE_SECRET)
        if not secret_id:
            return {}
        try:
            secret = self.model.get_secret(id=str(secret_id))
            content = secret.get_content(refresh=True)
        except (ops.SecretNotFoundError, ops.ModelError) as e:
            raise ExceptionWithStatusError(
                "Sensitive configuration secret is not accessible; "
                "check it exists and is granted to this charm.",
                ops.BlockedStatus,
            ) from e
        raw_json = content.get(sensitive_config.SENSITIVE_CONFIG_SECRET_KEY, "")
        try:
            return sensitive_config.parse_sensitive_config(raw_json)
        except sensitive_config.DuplicateSensitiveKeyError as e:
            raise ExceptionWithStatusError(str(e), ops.BlockedStatus) from e
        except json.JSONDecodeError as e:
            raise ExceptionWithStatusError(
                "Sensitive configuration secret payload is not valid JSON.",
                ops.BlockedStatus,
            ) from e

    # ---- reconcile --------------------------------------------------------

    def _reconcile(self, _: ops.EventBase) -> None:
        """Idempotent reconcile: configure git-sync and publish synced config."""
        try:
            self._validate_prerequisites()
        except ExceptionWithStatusError as e:
            # Prerequisites not met (e.g. git relation removed): stop git-sync so
            # it doesn't keep polling a stale/unrelated repo, then report status.
            logger.error(e)
            self._stop_git_sync()
            self.unit.status = e.status
            return
        try:
            self._configure_pebble_layer()
            self._publish_configuration()
        except ExceptionWithStatusError as e:
            # Prerequisites are met and git-sync is configured; a later failure
            # (e.g. file not yet synced) should not stop git-sync, so it can pick
            # up the content once it appears.
            logger.error(e)
            self.unit.status = e.status
            return
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
        """Push git credentials and the exechook, then (re)configure git-sync."""
        self._push_git_credentials()
        self._container.push(
            EXECHOOK_SCRIPT_PATH,
            self._exechook_script(),
            make_dirs=True,
            permissions=0o755,
        )
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

    def _publish_configuration(self) -> None:
        """Read the synced .ini and publish it over the relation.

        Combines the non-sensitive .ini (synced from git) with the sensitive
        values (from the user secret) into a template + sensitive map, and
        publishes via the provider interface.

        Raises:
            ExceptionWithStatusError: if the file is missing, or the sensitive
                secret is set but unreadable / invalid / has a collision.
        """
        ini_content = self._read_synced_file()
        sensitive_data = self._sensitive_data()
        template, flat_sensitive = config_generator.build_template_and_secrets(
            ini_content, sensitive_data=sensitive_data
        )
        self._config_provider.set_configuration(
            provider_configuration=template,
            provider_configuration_sensitive_data=flat_sensitive,
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

    def _exechook_script(self) -> str:
        """The script git-sync runs after each changed sync; fires a Pebble notice."""
        return (
            "#!/bin/sh\n"
            "# Notify the charm that git-sync fetched new content.\n"
            f"exec pebble notify {CONTENT_SYNCED_NOTICE_KEY}\n"
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

        Runs git-sync as a long-lived service that re-syncs every `--period` and
        runs the exechook on each changed sync. For HTTPS auth only the username
        is passed here; the token is supplied via a password file (see
        _git_sync_environment) so it never appears on the command line / process
        list. Arguments are joined with shlex.join so a relation value containing
        whitespace cannot inject extra flags.
        """
        assert git_info is not None  # guaranteed by _validate_prerequisites
        parts = [
            "/bin/git-sync",
            f"--repo={git_info.repository_url}",
            f"--root={GIT_SYNC_ROOT}",
            f"--link={GIT_SYNC_DEST}",
            f"--period={self._sync_period}",
            f"--exechook-command={EXECHOOK_SCRIPT_PATH}",
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
