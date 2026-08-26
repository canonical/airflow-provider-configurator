# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Shared fixtures for unit tests."""

import ops.testing
import pytest

from charm import AirflowProviderConfiguratorCharm

TEST_MODEL = ops.testing.Model(name="test-model", uuid="00000000-0000-0000-0000-000000000001")


@pytest.fixture
def context():
    """Return a testing Context for the charm."""
    return ops.testing.Context(charm_type=AirflowProviderConfiguratorCharm)


@pytest.fixture
def state():
    """Return a baseline testing State: a leader unit in a known model."""
    return ops.testing.State(leader=True, model=TEST_MODEL)
