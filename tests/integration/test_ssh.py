# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for SSH git authentication.

These exercise the parts of the SSH path that unit tests cannot reach: whether
the pinned git-sync image actually contains a working ``ssh``/``ssh-keyscan``,
whether the UID the workload runs as can read the key the charm writes, and
whether git-sync accepts the flag combination the charm builds.

They deliberately use a throwaway key that authorizes nothing, so the suite
needs no credential and runs the same for outside contributors. That still
covers everything above, because SSH distinguishes the failure modes: reaching
"Permission denied (publickey)" means the key was found, read and offered, and
the host key was verified -- only the final authorization was refused. A broken
key file, an unreadable one, or a failed host verification each produce a
different, recognisable error instead (see test_ssh_stack_works_end_to_end).

Because the key is rejected, git-sync cannot complete a fetch and the charm
settles into a blocked status (see _rejected_key_steady_state). That blocked
state is what these tests wait for; the credential files and the Pebble plan are
all written before the service is started, so they remain inspectable.
"""

import logging
import subprocess
import time

import jubilant
import pytest
from conftest import (
    APP_NAME,
    GIT_INTEGRATOR_APP,
    GIT_INTEGRATOR_CHANNEL,
    GIT_SYNC_IMAGE,
    WORKLOAD_CONTAINER,
)

logger = logging.getLogger(__name__)

# A real, reachable SSH git host. Only its *host key* is used anonymously (via
# ssh-keyscan); no test authenticates against it.
SSH_REPO_URL = "git@github.com:canonical/airflow-provider-configurator.git"
HTTPS_REPO_URL = "https://github.com/canonical/airflow-provider-configurator"

SSH_KEY_FILE = "/git-creds/ssh_key"
KNOWN_HOSTS_FILE = "/git-creds/known_hosts"

# The throwaway key authenticates nothing, so git-sync fails each fetch and
# exits (--max-failures defaults to 0) and Pebble restarts it. The process is
# therefore not reliably alive, so flags are read from the Pebble plan, which is
# stable, and evidence that git-sync *accepted* them comes from its logs.
_PEBBLE_PLAN = "/charm/bin/pebble plan"
_PEBBLE_LOGS = "/charm/bin/pebble logs git-sync -n 200"

INVALID_SSH_URL_MESSAGE = (
    "Git relation selected SSH authentication but the repository URL is not an SSH URL; "
    "use ssh://user@host/path or user@host:path"
)
GIT_SYNC_START_FAILED_MESSAGE = (
    "git-sync failed to start; check the repository URL and that the supplied "
    "credentials are accepted by the remote"
)
FILE_NOT_FOUND_PREFIX = "Configuration file not found at"


def _blocked_with(status, message: str) -> bool:
    """Return whether the app is blocked with exactly ``message``."""
    return jubilant.all_blocked(status, APP_NAME) and (
        status.apps[APP_NAME].app_status.message == message
    )


def _rejected_key_steady_state(status) -> bool:
    """Return whether the app settled into a state the throwaway key can produce.

    Two blocked messages are legitimate here and the charm alternates between
    them, so waiting for either avoids a race. Pebble restarts git-sync after it
    exits, so whether a given hook observes the failed start or an already
    restarted service depends on timing: if the start fails the charm reports
    GIT_SYNC_START_FAILED_MESSAGE, and if it does not, the charm gets further
    and reports the file it could never have synced as missing.
    """
    if not jubilant.all_blocked(status, APP_NAME):
        return False
    message = status.apps[APP_NAME].app_status.message
    return message == GIT_SYNC_START_FAILED_MESSAGE or message.startswith(FILE_NOT_FOUND_PREFIX)


def _wait_for_rejected_key(juju: jubilant.Juju) -> None:
    """Wait for the steady state produced by the throwaway key being refused."""
    juju.wait(_rejected_key_steady_state)


def _workload(juju: jubilant.Juju, command: str) -> str:
    """Run a command inside the git-sync workload container."""
    return juju.ssh(f"{APP_NAME}/0", command, container=WORKLOAD_CONTAINER)


def _configure_ssh(juju: jubilant.Juju, **overrides) -> None:
    """Point git-integrator at an SSH repository, with optional overrides.

    No tracking_ref is set, so git-sync follows the default branch. Pinning a
    branch name here would make the suite depend on this repository's branching
    scheme for no benefit: these tests never complete a clone.
    """
    config = {
        "authentication_method": "ssh",
        "repository_url": SSH_REPO_URL,
        # git-integrator raises KeyError if this is ever left unset.
        "ssh_strict_host_key_checking": True,
        # Explicit, so a test that overrides the port cannot leak it into the
        # next one: leaving it unset keeps the previous value.
        "ssh_port": 22,
    }
    config.update(overrides)
    juju.config(GIT_INTEGRATOR_APP, config)


def test_deploy(juju: jubilant.Juju, charm, throwaway_ssh_key: str):
    """Deploy the charm with git-integrator configured for SSH."""
    key_secret_uri = juju.add_secret("apc-ssh-key", {"ssh-private-key": throwaway_ssh_key})

    juju.deploy(
        charm.resolve(),
        app=APP_NAME,
        resources={"git-sync-image": GIT_SYNC_IMAGE},
        config={"airflow_provider_configurations_file_path": "providers.ini"},
    )
    juju.deploy(GIT_INTEGRATOR_APP, channel=GIT_INTEGRATOR_CHANNEL)
    juju.grant_secret(key_secret_uri, GIT_INTEGRATOR_APP)

    # ssh_strict_host_key_checking must be set explicitly: git-integrator reads
    # it unconditionally and raises KeyError when it is unset.
    _configure_ssh(
        juju,
        ssh_private_key_secret=key_secret_uri,
        ssh_strict_host_key_checking=True,
    )
    juju.integrate(f"{APP_NAME}:remote-airflow-provider-configurations", GIT_INTEGRATOR_APP)

    # The throwaway key is refused, so git-sync cannot complete a fetch and the
    # charm blocks. That is the expected steady state, and it is reached only
    # after the credentials and the Pebble layer have been written, which is
    # what the rest of the suite inspects.
    _wait_for_rejected_key(juju)


def test_git_sync_runs_as_a_uid_that_can_read_the_key(juju: jubilant.Juju):
    """The key is written 0400, so the workload UID must be the file's owner.

    A mock cannot catch this: it depends on the UID the pinned image actually
    runs as. If the image ever stops running as the key's owner, SSH breaks with
    a permissions error, and ``--add-user`` would not save it -- that flag only
    adds a /etc/passwd record so ssh can resolve an arbitrary UID, it does not
    grant read access.
    """
    key_stat = _workload(juju, f"stat -c '%a %U %u' {SSH_KEY_FILE}").strip()
    mode, owner, _ = key_stat.split()
    assert mode == "400", f"expected the key to stay private, got mode {mode}"

    runtime_uid = _workload(juju, "id -un").strip()
    assert runtime_uid == owner, (
        f"git-sync runs as {runtime_uid!r} but the key is owned by {owner!r}; "
        "a 0400 key is unreadable by anyone else"
    )
    # Prove readability rather than inferring it from the mode and owner.
    _workload(juju, f"head -c1 {SSH_KEY_FILE} >/dev/null")


def test_known_hosts_is_pinned_from_a_real_keyscan(juju: jubilant.Juju):
    """ssh-keyscan must exist in the image and return a usable host key."""
    known_hosts = _workload(juju, f"cat {KNOWN_HOSTS_FILE}")
    assert "github.com" in known_hosts
    assert "ssh-ed25519" in known_hosts or "ssh-rsa" in known_hosts

    mode = _workload(juju, f"stat -c '%a' {KNOWN_HOSTS_FILE}").strip()
    assert mode == "644", f"known_hosts should be world readable, got {mode}"


def test_git_sync_accepts_the_charms_ssh_flags(juju: jubilant.Juju):
    """git-sync must parse the charm's flag set rather than reject it.

    git-sync validates flags at startup and exits immediately on an unknown or
    malformed one, before any fetch. So the proof that the flags are valid for
    the pinned version is that it got far enough to attempt authentication.
    """
    plan = _workload(juju, _PEBBLE_PLAN)
    assert f"--ssh-key-file={SSH_KEY_FILE}" in plan
    assert f"--ssh-known-hosts-file={KNOWN_HOSTS_FILE}" in plan
    assert "--add-user" in plan

    logs = _workload(juju, _PEBBLE_LOGS)
    assert "invalid flag" not in logs and "unknown flag" not in logs
    assert "git fetch" in logs, "git-sync never reached a fetch, so it rejected its flags"


def test_ssh_stack_works_end_to_end(juju: jubilant.Juju):
    """The whole SSH path works; only authorization is refused.

    This is the core assertion of the module. "Permission denied (publickey)"
    is reached *after* ssh has loaded the key file and verified the host key, so
    it proves both worked. The negative assertions matter just as much: a
    different error here would mean a real defect rather than the expected
    rejection of an unregistered key.
    """
    logs = _workload(juju, _PEBBLE_LOGS)
    assert "Permission denied (publickey)" in logs, (
        "git-sync never reached an authentication decision; "
        "the SSH key or host verification failed before that"
    )
    # Host verification succeeded, so the pin is genuinely in use.
    assert "Host key verification failed" not in logs
    # The key was loaded, so it is neither missing nor rejected for bad modes.
    assert "bad permissions" not in logs
    assert "no such identity" not in logs


def test_blocks_when_ssh_url_is_not_an_ssh_url(juju: jubilant.Juju):
    """SSH auth with an https:// URL is a misconfiguration, not a sync failure."""
    _configure_ssh(juju, repository_url=HTTPS_REPO_URL)
    juju.wait(lambda status: _blocked_with(status, INVALID_SSH_URL_MESSAGE))
    _configure_ssh(juju)
    _wait_for_rejected_key(juju)


def test_disabling_strict_host_key_checking_removes_the_pin(juju: jubilant.Juju):
    """Turning verification off must drop known_hosts, not leave a stale pin."""
    _configure_ssh(juju, ssh_strict_host_key_checking=False)
    _wait_for_rejected_key(juju)

    plan = _workload(juju, _PEBBLE_PLAN)
    assert "--ssh-known-hosts=false" in plan
    present = _workload(juju, f"test -e {KNOWN_HOSTS_FILE} && echo present || echo absent")
    assert present.strip() == "absent"

    _configure_ssh(juju, ssh_strict_host_key_checking=True)
    _wait_for_rejected_key(juju)


def test_custom_ssh_port_is_folded_into_the_url_and_the_pin(juju: jubilant.Juju):
    """A non-default port must reach both --repo and the known_hosts marker.

    GitHub serves SSH on 443 via ssh.github.com, so this uses a real host/port
    pair and therefore a real keyscan, rather than asserting on string shapes.
    """
    _configure_ssh(
        juju,
        repository_url="git@ssh.github.com:canonical/airflow-provider-configurator.git",
        ssh_port=443,
    )
    _wait_for_rejected_key(juju)

    plan = _workload(juju, _PEBBLE_PLAN)
    expected_repo = (
        "--repo=ssh://git@ssh.github.com:443/canonical/airflow-provider-configurator.git"
    )
    assert expected_repo in plan

    known_hosts = _workload(juju, f"cat {KNOWN_HOSTS_FILE}")
    assert "[ssh.github.com]:443" in known_hosts

    _configure_ssh(juju)
    _wait_for_rejected_key(juju)


# This test must stay LAST. The upstream bug it trips leaves git-integrator in
# an error state that stops it applying any further config, so every test that
# ran after it would fail for reasons unrelated to what it checks.
#
# There is deliberately no integration test for "SSH selected but no key": the
# real provider blocks itself and publishes nothing in that case, so the charm
# never observes it. test_charm.py covers that message directly.
@pytest.mark.xfail(
    strict=False,
    reason=(
        "git-integrator (1.0/edge rev 5) never propagates a rotated key: its "
        "GitProviderEventHandler does not override _on_secret_changed_event, so the "
        "abstract base raises NotImplementedError and the secret-changed hook fails. "
        "This charm rewrites the key file correctly once it is published."
    ),
)
def test_key_rotation_rewrites_the_file_without_a_restart(juju: jubilant.Juju, tmp_path):
    """A rotated key reaches the container, and git-sync keeps using the file.

    Rotation changes nothing in the Pebble layer, so this also confirms the
    charm does not need to restart git-sync for a new key to take effect.

    The secret is updated in place, which is how an operator rotates a key.
    Pointing git-integrator at a different secret is not equivalent: it fails
    separately upstream (the fixed 'ssh-private-key' secret label is already
    taken by the first secret).
    """
    before = _workload(juju, f"sha256sum {SSH_KEY_FILE}").split()[0]

    rotated = tmp_path / "rotated_ed25519"
    subprocess.run(
        ["ssh-keygen", "-t", "ed25519", "-N", "", "-C", "rotated", "-f", str(rotated)],
        check=True,
        capture_output=True,
    )
    juju.update_secret("apc-ssh-key", {"ssh-private-key": rotated.read_text()})

    # Poll: waiting on status alone returns at once, because the charm is
    # already in its steady state before the rotation has had any effect.
    deadline = time.monotonic() + 120
    after = before
    while time.monotonic() < deadline and after == before:
        time.sleep(5)
        after = _workload(juju, f"sha256sum {SSH_KEY_FILE}").split()[0]

    assert after != before, "the rotated key never reached the workload container"
    assert _workload(juju, f"stat -c '%a' {SSH_KEY_FILE}").strip() == "400"
