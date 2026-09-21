# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Constants for the Airflow Provider Configurator charm."""

# Relations and workload.
GIT_RELATION_NAME = "remote-airflow-provider-configurations"
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
