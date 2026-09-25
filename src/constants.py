# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Constants for the Airflow Provider Configurator charm."""

# Relations and workload.
GIT_RELATION_NAME = "remote-airflow-provider-configurations"
# Relation over which the provider configuration is published to consumers
# (the airflow-coordinator charm).
PROVIDER_RELATION_NAME = "airflow-provider-configuration"
# Peer relation used to store the hash of the last published configuration, so
# republishing is skipped when nothing changed (spec 1.2).
PEER_RELATION_NAME = "replicas"
# Key under the peer app databag holding that hash.
PEER_CONFIG_HASH_KEY = "published-config-hash"
WORKLOAD_CONTAINER = "git-sync"
GIT_SYNC_SERVICE = "git-sync"
GIT_SYNC_ROOT = "/git"
# Symlink name under the root that git-sync points at the latest checkout.
GIT_SYNC_DEST = "repo"
# Private, root-only file holding the git PAT so it never appears on the command
# line or in the service environment.
GIT_SYNC_PASSWORD_FILE = "/git-creds/password"
# Script git-sync runs (via --exechook-command) after each changed sync.
EXECHOOK_SCRIPT_PATH = "/usr/local/bin/notify-content-synced"
# Pebble custom-notice key fired by the exechook when content changes.
CONTENT_SYNCED_NOTICE_KEY = "canonical.com/airflow-provider-configurator/content-synced"
# Signal git-sync listens on (via --sync-on-signal) to run a sync immediately
# instead of waiting for the next --period tick. Used by the sync-now action so
# the already-running poller does the fetch: starting a second git-sync against
# the same --root is unsafe, because git-sync empties its root on startup.
GIT_SYNC_SIGNAL = "SIGHUP"

# git-sync's Prometheus endpoint (enabled with --http-bind/--http-metrics), used
# by the sync-now action to tell when the sync it triggered has finished.
#
# --touch-file is deliberately NOT used for this. Despite git-sync's manual
# describing it as touched "whenever a sync completes", its source only touches
# it when the synced content actually changed, so an unchanged repository (the
# common case) would never signal completion. The git_sync_count_total counter
# is incremented once per completed sync attempt in all three outcomes
# (success, noop, error), which is what the action actually needs to observe.
#
# Bound to loopback so the metrics are reachable from the charm container (which
# shares the pod's network namespace) without being exposed outside the pod.
GIT_SYNC_METRICS_HOST = "127.0.0.1"
GIT_SYNC_METRICS_PORT = 9148
GIT_SYNC_METRICS_PATH = "/metrics"
# How long (seconds) to wait on a single read of the metrics endpoint.
GIT_SYNC_METRICS_TIMEOUT_SECONDS = 5
# Counter metric, labelled by outcome: success, noop or error.
GIT_SYNC_COUNT_METRIC = "git_sync_count_total"

# How long (seconds) the sync-now action waits for the signalled sync to finish
# before failing, so the action fails cleanly rather than hanging the hook on a
# slow or unreachable repository.
SYNC_NOW_TIMEOUT_SECONDS = 120
# How long (seconds) to wait between checks of the sync counter while waiting.
SYNC_NOW_POLL_INTERVAL_SECONDS = 1

# Config option keys.
CONFIG_FILE_PATH = "airflow_provider_configurations_file_path"
CONFIG_SYNC_PERIOD = "airflow_provider_configurations_sync_period"
CONFIG_SENSITIVE_SECRET = "airflow_provider_configurations_secret"

# Status messages (centralised so tests can assert exact equality).
MISSING_FILE_PATH_MESSAGE = f"Missing required config: {CONFIG_FILE_PATH}"
MISSING_GIT_RELATION_MESSAGE = (
    "Missing git relation; relate to a git provider (e.g. git-integrator)"
)
INVALID_GIT_RELATION_MESSAGE = "Git relation has not provided valid repository information"
SSH_NOT_SUPPORTED_MESSAGE = "SSH authentication is not supported yet; use HTTPS or a public repo"
WAITING_FOR_CONTAINER_MESSAGE = "Waiting for the git-sync container"
MISSING_SENSITIVE_KEY_MESSAGE = (
    "Sensitive configuration secret is missing the "
    "'airflow-provider-configurations' key"
)
SYNC_NOW_NOT_RUNNING_MESSAGE = "git-sync is not running; cannot force a sync"
SYNC_NOW_TIMEOUT_MESSAGE = "Forced git-sync did not complete in time"
SYNC_NOW_FETCH_FAILED_MESSAGE = (
    "Forced git-sync failed to fetch from the repository; "
    "check the git-sync logs and the repository configuration"
)
