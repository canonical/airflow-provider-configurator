# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Shared fixtures for unit tests."""

import airflow_provider_configurator as apc
import ops
import ops.testing
import pytest

from charm import AirflowProviderConfiguratorCharm

TEST_MODEL = ops.testing.Model(name="test-model", uuid="00000000-0000-0000-0000-000000000001")

RELATION_NAME = "airflow-provider-configuration"
RELATION_INTERFACE = "airflow_provider_configuration"


@pytest.fixture
def context():
    """Return a testing Context for the charm."""
    return ops.testing.Context(charm_type=AirflowProviderConfiguratorCharm)


@pytest.fixture
def state():
    """Return a baseline testing State: a leader unit in a known model."""
    return ops.testing.State(leader=True, model=TEST_MODEL)


class ProviderHarnessCharm(ops.CharmBase):
    """Mock charm wiring the provider side of the interface."""

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = apc.AirflowProviderConfiguratorProvides(self, RELATION_NAME)


class RequirerHarnessCharm(ops.CharmBase):
    """Mock charm wiring the requirer side of the interface."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = apc.AirflowProviderConfiguratorRequires(self, RELATION_NAME)


@pytest.fixture
def provider_context():
    """Return a testing Context wiring the provider side of the interface."""
    return ops.testing.Context(
        charm_type=ProviderHarnessCharm,
        meta={
            "name": "airflow-provider-configurator",
            "provides": {RELATION_NAME: {"interface": RELATION_INTERFACE}},
        },
    )


@pytest.fixture
def requirer_context():
    """Return a testing Context wiring the requirer side of the interface."""
    return ops.testing.Context(
        charm_type=RequirerHarnessCharm,
        meta={
            "name": "airflow-coordinator",
            "requires": {RELATION_NAME: {"interface": RELATION_INTERFACE, "limit": 1}},
        },
    )


@pytest.fixture
def relation():
    """Return a bare provider-configuration relation."""
    return ops.testing.Relation(RELATION_NAME, interface=RELATION_INTERFACE)
