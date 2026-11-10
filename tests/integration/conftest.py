# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import logging
import os
import pathlib
import subprocess
import sys
import time

import jubilant
import pytest
import yaml

logger = logging.getLogger(__name__)

CHARMCRAFT_FILE = yaml.safe_load(pathlib.Path("./charmcraft.yaml").read_text())
GIT_SYNC_IMAGE = CHARMCRAFT_FILE["resources"]["git-sync-image"]["upstream-source"]

APP_NAME = "airflow-provider-configurator"
GIT_INTEGRATOR_APP = "git-integrator"
GIT_INTEGRATOR_CHANNEL = "1.0/edge"
WORKLOAD_CONTAINER = "git-sync"


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Create a temporary Juju model for running tests."""
    if "JUJU_MODEL" in os.environ:
        juju = jubilant.Juju(wait_timeout=10 * 60)
        juju.add_model(os.environ["JUJU_MODEL"], config={"update-status-hook-interval": "10s"})
        yield juju
        _dump_logs_on_failure(request, juju)
        return

    with jubilant.temp_model(config={"update-status-hook-interval": "10s"}) as juju:
        juju.wait_timeout = 10 * 60
        yield juju
        _dump_logs_on_failure(request, juju)


def _dump_logs_on_failure(request: pytest.FixtureRequest, juju: jubilant.Juju) -> None:
    """Print the Juju debug log when a test in the module failed."""
    if request.session.testsfailed:
        logger.info("Collecting Juju logs...")
        time.sleep(0.5)  # Wait for Juju to process logs.
        print(juju.debug_log(limit=1000), end="", file=sys.stderr)


@pytest.fixture(scope="session")
def charm():
    """Return the path of the charm under test."""
    if "CHARM_PATH" in os.environ:
        charm_path = pathlib.Path(os.environ["CHARM_PATH"])
        if not charm_path.exists():
            raise FileNotFoundError(f"Charm does not exist: {charm_path}")
        return charm_path

    charm_paths = list(pathlib.Path(".").glob(f"{APP_NAME}*.charm"))
    if not charm_paths:
        raise FileNotFoundError(f"No {APP_NAME} .charm file in current directory")
    if len(charm_paths) > 1:
        path_list = ", ".join(str(path) for path in charm_paths)
        raise ValueError(f"More than one {APP_NAME} .charm file in current directory: {path_list}")
    return charm_paths[0]


@pytest.fixture(scope="session")
def throwaway_ssh_key(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Generate an SSH private key that exists only for this test session.

    The tests deliberately never register this key anywhere, so it authenticates
    nothing. That is the point: it exercises the whole SSH path (key file is
    written, read and offered; the host is verified) up to the authorization
    decision, without the suite needing a real credential. See
    tests/integration/test_ssh.py for why that distinction is load bearing.
    """
    key_path = tmp_path_factory.mktemp("ssh") / "id_ed25519"
    subprocess.run(
        ["ssh-keygen", "-t", "ed25519", "-N", "", "-C", "apc-integration", "-f", str(key_path)],
        check=True,
        capture_output=True,
    )
    return key_path.read_text()
