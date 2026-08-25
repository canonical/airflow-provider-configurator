# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Unit tests for the Airflow Provider Configurator charm."""

import ops
import pytest


@pytest.mark.parametrize("leader", [True, False])
def test_charm_is_active(context, state, leader):
    """The charm reaches active status on start, whether leader or not."""
    import dataclasses

    state = dataclasses.replace(state, leader=leader)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.ActiveStatus()


def test_reconcile_on_config_changed(context, state):
    """The charm stays active after a config-changed event."""
    state_out = context.run(context.on.config_changed(), state)

    assert state_out.unit_status == ops.ActiveStatus()


def test_reconcile_on_update_status(context, state):
    """The charm stays active after an update-status event."""
    state_out = context.run(context.on.update_status(), state)

    assert state_out.unit_status == ops.ActiveStatus()
