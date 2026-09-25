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

import hashlib
import http.client
import json
import logging
import re
import shlex
import time
from pathlib import PurePosixPath

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
    GIT_SYNC_COUNT_METRIC,
    GIT_SYNC_DEST,
    GIT_SYNC_METRICS_HOST,
    GIT_SYNC_METRICS_PATH,
    GIT_SYNC_METRICS_PORT,
    GIT_SYNC_METRICS_TIMEOUT_SECONDS,
    GIT_SYNC_PASSWORD_FILE,
    GIT_SYNC_ROOT,
    GIT_SYNC_SERVICE,
    GIT_SYNC_SIGNAL,
    INVALID_GIT_RELATION_MESSAGE,
    MISSING_FILE_PATH_MESSAGE,
    MISSING_GIT_RELATION_MESSAGE,
    MISSING_SENSITIVE_KEY_MESSAGE,
    PEER_CONFIG_HASH_KEY,
    PEER_RELATION_NAME,
    PROVIDER_RELATION_NAME,
    SSH_NOT_SUPPORTED_MESSAGE,
    SYNC_NOW_FETCH_FAILED_MESSAGE,
    SYNC_NOW_NOT_RUNNING_MESSAGE,
    SYNC_NOW_POLL_INTERVAL_SECONDS,
    SYNC_NOW_TIMEOUT_MESSAGE,
    SYNC_NOW_TIMEOUT_SECONDS,
    WAITING_FOR_CONTAINER_MESSAGE,
    WORKLOAD_CONTAINER,
)

logger = logging.getLogger(__name__)


class ExitWithStatusError(Exception):
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
            self.on[WORKLOAD_CONTAINER].pebble_ready,
            self.on[WORKLOAD_CONTAINER].pebble_custom_notice,
            self.on[GIT_RELATION_NAME].relation_changed,
            # A consumer that has just joined has an empty databag, so it must be
            # served here: the content hash is unchanged from the last publish, so
            # without this event nothing would write to the new relation until an
            # unrelated event (at worst the next update-status) happened to
            # reconcile.
            self.on[PROVIDER_RELATION_NAME].relation_joined,
        ):
            self.framework.observe(event, self._reconcile)

        self.framework.observe(self.on.secret_changed, self._on_secret_changed)
        self.framework.observe(self.on.sync_now_action, self._on_sync_now_action)

    # ---- config accessors -------------------------------------------------

    @property
    def _file_path(self) -> str | None:
        """The configured path to the provider .ini file, or None if unset."""
        value = self.config.get(CONFIG_FILE_PATH)
        return str(value) if value else None

    @property
    def _sensitive_secret_id(self) -> str | None:
        """The configured user-secret id holding sensitive values, or None if unset."""
        value = self.config.get(CONFIG_SENSITIVE_SECRET)
        return str(value) if value else None

    @property
    def _resolved_file_path(self) -> str:
        """The configured file path, resolved against the git relation's `path`.

        The git relation may advertise an optional `path`: a subdirectory within
        the repository that scopes where the provider config lives. git-sync
        checks the whole repo out under GIT_SYNC_ROOT/GIT_SYNC_DEST, so `path`
        only matters at read time — the configured file is resolved relative to
        that subdirectory when one is advertised.
        """
        file_path = self._file_path or ""
        git_info = self._git_connection_info()
        subdir = (git_info.path if git_info else None) or ""
        return str(PurePosixPath(subdir) / file_path) if subdir else file_path

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
            ExitWithStatusError: if the secret is set but cannot be read or
                parsed, or if a duplicate section.option is found (spec 3.3).
        """
        secret_id = self._sensitive_secret_id
        if not secret_id:
            return {}
        try:
            secret = self.model.get_secret(id=secret_id)
            content = secret.get_content(refresh=True)
        except (ops.SecretNotFoundError, ops.ModelError) as e:
            raise ExitWithStatusError(
                "Sensitive configuration secret is not accessible; "
                "check it exists and is granted to this charm.",
                ops.BlockedStatus,
            ) from e
        raw_json = content.get(sensitive_config.SENSITIVE_CONFIG_SECRET_KEY)
        if raw_json is None:
            # The operator explicitly configured a secret, so a missing payload key
            # is a mistake rather than "no sensitive configuration": treating it as
            # empty would silently drop the intended values, and could even trigger
            # the empty-config cleanup when the .ini is empty too. Block instead.
            raise ExitWithStatusError(MISSING_SENSITIVE_KEY_MESSAGE, ops.BlockedStatus)
        try:
            return sensitive_config.parse_sensitive_config(raw_json)
        except (
            sensitive_config.DuplicateSensitiveKeyError,
            sensitive_config.InvalidSensitiveConfigError,
        ) as e:
            raise ExitWithStatusError(str(e), ops.BlockedStatus) from e
        except json.JSONDecodeError as e:
            raise ExitWithStatusError(
                "Sensitive configuration secret payload is not valid JSON.",
                ops.BlockedStatus,
            ) from e

    # ---- reconcile --------------------------------------------------------

    def _reconcile(self, _: ops.EventBase, *, force_publish: bool = False) -> None:
        """Idempotent reconcile: configure git-sync and publish synced config."""
        try:
            self._validate_prerequisites()
        except ExitWithStatusError as e:
            # Prerequisites not met (e.g. git relation removed): stop git-sync so
            # it doesn't keep polling a stale/unrelated repo, then report status.
            logger.error(e)
            self._stop_git_sync()
            if self._config_source_removed():
                # The source of the configuration is gone, so the configuration we
                # published is no longer backed by anything. Empty the databag and
                # revoke the secret straight away, rather than leaving the
                # coordinator applying config we can no longer refresh; seeing it
                # empty is what makes the coordinator reconfigure (spec 2.2, 4.2).
                self._clear_configuration()
            self.unit.status = e.status
            return
        try:
            self._configure_pebble_layer()
            self._publish_configuration(force=force_publish)
        except ExitWithStatusError as e:
            # Prerequisites are met and git-sync is configured; a later failure
            # (e.g. file not yet synced) should not stop git-sync, so it can pick
            # up the content once it appears.
            logger.error(e)
            self.unit.status = e.status
            return
        self.unit.status = ops.ActiveStatus()

    def _on_secret_changed(self, event: ops.SecretChangedEvent) -> None:
        """Reconcile, forcing the publish past the content-hash dedup.

        A new revision of the user secret can carry different sensitive values
        under exactly the same options, which leaves the config hash identical
        (see _config_hash for why the values are not part of it). This event is
        the only indication that those values moved, so it must not be deduped
        away or the coordinator would keep rendering the superseded revision.
        """
        self._reconcile(event, force_publish=True)

    def _config_source_removed(self) -> bool:
        """Whether the configuration source itself is gone, not just unavailable.

        True when the git relation has been removed or the file path config has
        been unset — the operator has taken the source away, so what we published
        must be withdrawn. False for transient conditions such as the container
        not being ready yet, where the existing configuration is still valid and
        withdrawing it would cause needless churn on the coordinator.
        """
        return not self._file_path or self.model.get_relation(GIT_RELATION_NAME) is None

    def _clear_configuration(self) -> None:
        """Withdraw the published configuration and forget its hash.

        Clearing the stored hash keeps `_publish_configuration`'s dedup honest:
        the peer databag records the hash of what is currently published, and
        after this nothing is.
        """
        self._config_provider.clear_configuration()
        self._clear_stored_config_hash()

    def _validate_prerequisites(self) -> None:
        """Check required config, the git relation, and the container are ready.

        Raises:
            ExitWithStatusError: if any prerequisite is not met.
        """
        if not self._file_path:
            raise ExitWithStatusError(MISSING_FILE_PATH_MESSAGE, ops.BlockedStatus)
        if self.model.get_relation(GIT_RELATION_NAME) is None:
            raise ExitWithStatusError(MISSING_GIT_RELATION_MESSAGE, ops.BlockedStatus)
        git_info = self._git_connection_info()
        if git_info is None:
            raise ExitWithStatusError(INVALID_GIT_RELATION_MESSAGE, ops.BlockedStatus)
        if git_info.authentication_method == git.AuthenticationMethodEnum.SSH:
            raise ExitWithStatusError(SSH_NOT_SUPPORTED_MESSAGE, ops.BlockedStatus)
        if not self._container.can_connect():
            raise ExitWithStatusError(WAITING_FOR_CONTAINER_MESSAGE, ops.WaitingStatus)

    def _configure_pebble_layer(self) -> None:
        """Push git credentials and the exechook, then (re)configure git-sync."""
        self._push_git_credentials()
        try:
            self._container.push(
                EXECHOOK_SCRIPT_PATH,
                self._exechook_script(),
                make_dirs=True,
                permissions=0o755,
            )
        except ops.pebble.PathError as e:
            raise ExitWithStatusError(
                "Failed to write the git-sync exechook to the workload container.",
                ops.BlockedStatus,
            ) from e
        # Only replan when the git-sync service definition actually changes.
        # Every event (including update-status) reaches here, and an
        # unconditional replan would repeatedly re-issue Pebble operations and
        # risk disturbing the continuously running sync service for no reason.
        # Compare the effective plan before and after applying the layer so the
        # comparison uses Pebble's own normalized view rather than a hand-rolled
        # diff of a raw layer against a combined plan.
        services_before = self._container.get_plan().services
        self._container.add_layer(GIT_SYNC_SERVICE, self._git_sync_layer, combine=True)
        services_after = self._container.get_plan().services
        if services_before != services_after:
            self._container.replan()

    def _stop_git_sync(self) -> None:
        """Stop the git-sync service so it doesn't keep polling a stale/unrelated repo.

        Called when prerequisites are no longer met (e.g. the git relation was
        removed). Safe to call when the container is unreachable or the service
        was never started. Also removes any stored git credentials so a rotated
        or revoked token does not linger once the relation is gone (the
        reconcile early-return means _push_git_credentials would not run).
        """
        if not self._container.can_connect():
            return
        if GIT_SYNC_SERVICE in self._container.get_services():
            self._container.stop(GIT_SYNC_SERVICE)
        self._container.remove_path(GIT_SYNC_PASSWORD_FILE, recursive=True)

    def _publish_configuration(self, *, force: bool = False) -> None:
        """Read the synced .ini and publish it over the relation.

        Combines the non-sensitive .ini (synced from git) with the sensitive
        values (from the user secret) into a template + sensitive map, and
        publishes via the provider interface.

        A hash of the resulting (template, sensitive map) is stored in the peer
        relation and compared on each call: when nothing has changed the publish
        is skipped entirely, avoiding relation/secret churn on every event or
        notice (spec 1.2). When the configuration is empty — no non-sensitive
        template and no sensitive data — the previously published configuration
        is cleared and its charm secret revoked (spec 2.2); this is the case of a
        file that is present but empty, as distinct from an absent file (handled
        as BlockedStatus in _read_synced_file, spec 1.3).

        Args:
            force: when True, bypass the content-hash deduplication and always
                (re)publish the current configuration. Used by the sync-now
                action so an operator can force a republish on demand even when
                the content is unchanged.

        Raises:
            ExitWithStatusError: if the file is missing, or the sensitive
                secret is set but unreadable / invalid / has a collision.
        """
        ini_content = self._read_synced_file()
        sensitive_data = self._sensitive_data()
        template, flat_sensitive = config_generator.build_template_and_secrets(
            ini_content, sensitive_data=sensitive_data
        )

        config_hash = self._config_hash(template, self._sensitive_secret_id)
        is_empty = not template and not flat_sensitive
        # Skip when the content is unchanged AND the relations already reflect the
        # desired state: for a non-empty config that means every relation carries
        # the data (is_published); for an empty config it means every relation is
        # already cleared (is_cleared). Checking the matching predicate keeps a
        # freshly-joined relation from being stranded, and stops the empty state
        # from re-running clear_configuration() on every event (spec 1.2, 2.2).
        # force=True (the sync-now action) skips this dedup entirely.
        already_reflected = (
            self._config_provider.is_cleared()
            if is_empty
            else self._config_provider.is_published()
        )
        if not force and config_hash == self._stored_config_hash and already_reflected:
            return

        if is_empty:
            # Both inputs empty (present-but-empty file and no sensitive data):
            # remove any previously published configuration (spec 2.2).
            self._config_provider.clear_configuration()
        else:
            self._config_provider.set_configuration(
                provider_configuration=template,
                provider_configuration_sensitive_data=flat_sensitive,
            )
        self._store_config_hash(config_hash)

    @staticmethod
    def _config_hash(template: str, sensitive_secret_id: str | None) -> str:
        """Return a stable hash of the publishable configuration.

        Covers the rendered template and the identity of the secret supplying the
        sensitive values; its sole purpose is deciding whether a republish is
        needed (spec 1.2). Keys are sorted so the hash is independent of dict
        ordering.

        The sensitive values themselves are deliberately excluded. This hash goes
        into the peer databag, which `juju show-unit` exposes to anyone with
        model access, and although a digest cannot be reversed it can be
        confirmed: a short or predictable value can be guessed, hashed and
        compared. The template is enough to notice a change in *which* options
        are sensitive, because every key in the sensitive map has a matching
        `{{ ... }}` placeholder in it, and the secret id covers the option being
        repointed at a different secret. The one change left over -- a new
        revision of the same secret -- cannot be seen here at all, because Juju
        only lets a secret's owner read its revision and this charm is merely an
        observer of it; _on_secret_changed handles that case instead.
        """
        payload = json.dumps(
            {"template": template, "sensitive-secret-id": sensitive_secret_id}, sort_keys=True
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    @property
    def _stored_config_hash(self) -> str | None:
        """The hash of the last published configuration, from the peer databag.

        Returns None if there is no peer relation yet or nothing has been
        published, in which case any computed hash counts as a change.
        """
        relation = self.model.get_relation(PEER_RELATION_NAME)
        if relation is None:
            return None
        return relation.data[self.app].get(PEER_CONFIG_HASH_KEY)

    def _store_config_hash(self, config_hash: str) -> None:
        """Persist the published-configuration hash in the peer app databag.

        Only the leader writes app data; on a non-leader this is a no-op, matching
        the interface methods that also only publish when leader.
        """
        if not self.unit.is_leader():
            return
        relation = self.model.get_relation(PEER_RELATION_NAME)
        if relation is None:
            return
        relation.data[self.app][PEER_CONFIG_HASH_KEY] = config_hash

    def _clear_stored_config_hash(self) -> None:
        """Forget the published-configuration hash in the peer app databag.

        Leader-only, mirroring _store_config_hash.
        """
        if not self.unit.is_leader():
            return
        relation = self.model.get_relation(PEER_RELATION_NAME)
        if relation is None:
            return
        relation.data[self.app].pop(PEER_CONFIG_HASH_KEY, None)

    def _read_synced_file(self) -> str:
        """Read the configured .ini from the synced git content.

        The file is located relative to the git relation's optional `path`
        subdirectory, so a repo that scopes its config under a sub-folder is
        honored (spec / review comment on the unused `path` field).

        Raises:
            ExitWithStatusError: if the file does not exist (spec 1.3).
        """
        resolved_path = self._resolved_file_path
        full_path = f"{GIT_SYNC_ROOT}/{GIT_SYNC_DEST}/{resolved_path}"
        try:
            return self._container.pull(full_path, encoding="utf-8").read()
        except ops.pebble.PathError as e:
            raise ExitWithStatusError(
                f"Configuration file not found at {resolved_path}; "
                "check the repository and file path.",
                ops.BlockedStatus,
            ) from e

    # ---- git-sync layer ---------------------------------------------------

    @staticmethod
    def _https_credentials(git_info: git.GitProviderModel | None) -> tuple[str, str] | None:
        """The (username, token) pair for HTTPS auth, or None if it is incomplete.

        git-sync refuses to start at all when it is given one half of the pair
        ("invalid flag: --password-file may only be specified when --username is
        specified"), so the two values have to be applied together or not at all.

        They do arrive apart in practice. The git integrator library only blanks
        the fields belonging to the authentication method that is *not* in use,
        and it blanks a token to the placeholder string "None" rather than to a
        falsy value, so a relation using SSH -- or one whose authentication
        method is not set yet -- can report a truthy token with no username.
        Requiring both here keeps those states from building a layer that git-sync
        rejects, which would otherwise fail the hook and leave the unit in error.
        """
        if git_info is None:
            return None
        username = git_info.credentials_username
        token = git_info.credentials_personal_access_token
        if not username or not token:
            return None
        return username, token

    def _push_git_credentials(self) -> None:
        """Write the git PAT to a private file read by git-sync.

        The token is stored in a root-only file inside the container and passed to
        git-sync via GITSYNC_PASSWORD_FILE, so it appears neither on the command
        line nor in the service environment.

        When no usable token is available (e.g. the relation dropped its
        credentials or switched to SSH) any previously written credentials file is
        removed so a stale secret does not linger in the container.
        """
        credentials = self._https_credentials(self._git_connection_info())
        if credentials is None:
            self._container.remove_path(GIT_SYNC_PASSWORD_FILE, recursive=True)
            return
        _, token = credentials
        try:
            self._container.push(GIT_SYNC_PASSWORD_FILE, token, make_dirs=True, permissions=0o400)
        except ops.pebble.PathError as e:
            raise ExitWithStatusError(
                "Failed to write git credentials to the workload container.",
                ops.BlockedStatus,
            ) from e

    def _exechook_script(self) -> str:
        """The script git-sync runs after each changed sync; fires a Pebble notice."""
        return (
            "#!/bin/sh\n"
            "# Notify the charm that git-sync fetched new content.\n"
            f"exec pebble notify {CONTENT_SYNCED_NOTICE_KEY}\n"
        )

    @property
    def _git_sync_layer(self) -> ops.pebble.Layer:
        """Build the Pebble layer that runs git-sync in continuous poll mode."""
        git_info = self._git_connection_info()
        if git_info is None:
            raise ExitWithStatusError(INVALID_GIT_RELATION_MESSAGE, ops.BlockedStatus)
        service: ops.pebble.ServiceDict = {
            "override": "replace",
            "summary": "git-sync",
            "command": self._git_sync_command(git_info),
            "startup": "enabled",
        }
        environment = self._git_sync_environment(git_info)
        if environment:
            service["environment"] = environment
        layer: ops.pebble.LayerDict = {
            "summary": "git-sync layer",
            "description": "Continuously sync provider configuration from git.",
            "services": {GIT_SYNC_SERVICE: service},
        }
        return ops.pebble.Layer(layer)

    def _git_sync_command(self, git_info: git.GitProviderModel) -> str:
        """Construct the git-sync command line for continuous polling.

        Runs git-sync as a long-lived service that re-syncs every `--period` and
        runs the exechook on each changed sync. For HTTPS auth only the username
        is passed here; the token is supplied via a password file (see
        _git_sync_environment) so it never appears on the command line / process
        list. Arguments are joined with shlex.join so a relation value containing
        whitespace cannot inject extra flags.

        `--sync-on-signal` and the metrics endpoint exist for the sync-now
        action: the signal makes this already-running process sync immediately
        instead of waiting for the next `--period` tick, and the counter exposed
        at `--http-bind` lets the action detect that the sync it asked for has
        finished (see _git_sync_counts for why `--touch-file` is unsuitable).
        """
        parts = [
            "/bin/git-sync",
            f"--repo={git_info.repository_url}",
            f"--root={GIT_SYNC_ROOT}",
            f"--link={GIT_SYNC_DEST}",
            f"--period={self._sync_period}",
            f"--exechook-command={EXECHOOK_SCRIPT_PATH}",
            f"--sync-on-signal={GIT_SYNC_SIGNAL}",
            f"--http-bind={GIT_SYNC_METRICS_HOST}:{GIT_SYNC_METRICS_PORT}",
            "--http-metrics",
        ]
        if git_info.tracking_ref:
            parts.append(f"--ref={git_info.tracking_ref}")
        credentials = self._https_credentials(git_info)
        if credentials is not None:
            parts.append(f"--username={credentials[0]}")
        return shlex.join(parts)

    def _git_sync_environment(self, git_info: git.GitProviderModel) -> dict[str, str]:
        """Environment for the git-sync service.

        The personal access token is referenced via GITSYNC_PASSWORD_FILE (a
        root-only file) rather than an inline value, so it is not exposed in the
        process list or the service environment. It is only set alongside the
        matching `--username`, which git-sync requires (see _https_credentials).
        """
        env: dict[str, str] = {}
        if self._https_credentials(git_info) is not None:
            env["GITSYNC_PASSWORD_FILE"] = GIT_SYNC_PASSWORD_FILE
        return env

    def _git_sync_counts(self) -> tuple[float, float] | None:
        """Completed git-sync attempts so far, as (total, errors).

        Reads git-sync's Prometheus endpoint and sums `git_sync_count_total`,
        which is incremented once per completed sync attempt and labelled with
        the outcome: `success` (content changed), `noop` (content unchanged) or
        `error` (the fetch failed).

        This is used instead of `--touch-file` because git-sync only touches
        that file when the content changed -- despite its manual describing it
        as touched "whenever a sync completes" -- so a sync of an unchanged
        repository would never be observable. The same limitation applies to the
        exechook, which is why it cannot be reused here either.

        Returns:
            (total, errors), or None if the endpoint could not be read (for
            example git-sync is starting up or has died).
        """
        connection = http.client.HTTPConnection(
            GIT_SYNC_METRICS_HOST,
            GIT_SYNC_METRICS_PORT,
            timeout=GIT_SYNC_METRICS_TIMEOUT_SECONDS,
        )
        try:
            connection.request("GET", GIT_SYNC_METRICS_PATH)
            response = connection.getresponse()
            if response.status != 200:
                return None
            payload = response.read().decode("utf-8", errors="replace")
        except (OSError, http.client.HTTPException):
            return None
        finally:
            connection.close()

        total = errors = 0.0
        pattern = re.compile(
            rf'^{re.escape(GIT_SYNC_COUNT_METRIC)}\{{status="(?P<status>[^"]*)"\}}\s+'
            r"(?P<value>[0-9.eE+-]+)\s*$"
        )
        for line in payload.splitlines():
            match = pattern.match(line.strip())
            if not match:
                continue
            try:
                value = float(match["value"])
            except ValueError:
                continue
            total += value
            if match["status"] == "error":
                errors += value
        return total, errors

    def _trigger_sync(self) -> None:
        """Make the running git-sync fetch from the remote now, and wait for it.

        Sends `--sync-on-signal`'s signal to the existing git-sync service rather
        than starting a second git-sync process: git-sync empties its `--root` on
        startup, so a concurrent one-time run against the same root could destroy
        the poller's working state. After signalling, this waits for git-sync's
        completed-sync counter to advance, which happens whether or not the
        content changed.

        Raises:
            ExitWithStatusError: if git-sync is not running, if the sync it
                performed failed, or if the sync does not complete within
                SYNC_NOW_TIMEOUT_SECONDS, so the action reports a clear failure
                instead of claiming success.
        """
        before_total, before_errors = self._git_sync_counts() or (0.0, 0.0)
        try:
            self._container.send_signal(GIT_SYNC_SIGNAL, GIT_SYNC_SERVICE)
        except (ops.pebble.APIError, ops.ModelError) as e:
            raise ExitWithStatusError(SYNC_NOW_NOT_RUNNING_MESSAGE, ops.BlockedStatus) from e

        deadline = time.monotonic() + SYNC_NOW_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            time.sleep(SYNC_NOW_POLL_INTERVAL_SECONDS)
            counts = self._git_sync_counts()
            if counts is None:
                # Endpoint momentarily unavailable; keep waiting until the
                # deadline rather than reporting a failure we cannot confirm.
                continue
            total, errors = counts
            if total < before_total:
                # Counters only reset when the process restarted, which git-sync
                # does after a failed sync (it exits once --max-failures is hit).
                raise ExitWithStatusError(SYNC_NOW_FETCH_FAILED_MESSAGE, ops.BlockedStatus)
            if total > before_total:
                if errors > before_errors:
                    raise ExitWithStatusError(SYNC_NOW_FETCH_FAILED_MESSAGE, ops.BlockedStatus)
                return
        raise ExitWithStatusError(SYNC_NOW_TIMEOUT_MESSAGE, ops.BlockedStatus)

    # ---- actions ----------------------------------------------------------

    def _on_sync_now_action(self, event: ops.ActionEvent) -> None:
        """Force an immediate git-sync fetch and republish, bypassing dedup.

        Implements the sync-now action (spec 1.4.2.1): it drives git-sync to
        fetch from the remote once and waits for that to finish (rather than
        waiting for the poller's next `--period` tick), then republishes the
        resulting configuration with dedup bypassed so the publish happens even
        when the content is unchanged. Fails cleanly (no false "republished")
        if prerequisites are unmet or the fetch fails.

        This deliberately does not go through `_reconcile`. `_reconcile` is the
        idempotent, converge-to-desired-state path: it skips work when nothing
        changed and reports problems via unit status. The action needs the
        opposite on both counts — it must republish even when nothing changed,
        and it must surface failures through `event.fail` so the operator sees
        them in the action result rather than only in unit status.
        """
        try:
            self._validate_prerequisites()
            self._trigger_sync()
            self._publish_configuration(force=True)
        except ExitWithStatusError as e:
            event.fail(e.message)
            return
        event.set_results({"result": "Provider configuration republished."})


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowProviderConfiguratorCharm)
