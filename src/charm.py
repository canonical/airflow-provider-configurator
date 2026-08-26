#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Provider Configurator charm application.

This charm lets Charmed Airflow operators configure Airflow providers without
manually editing airflow.cfg. This module currently provides a minimal skeleton:
the charm installs, reconciles, and reports an active status. Provider
configuration sourcing (git relation and Juju secret) and the relation to the
Airflow Coordinator charm are added in follow-up work.
"""

import logging

import ops

logger = logging.getLogger(__name__)


class AirflowProviderConfiguratorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        # Reconcile on any lifecycle event that could change the charm's desired state.
        for event in (
            self.on.install,
            self.on.start,
            self.on.config_changed,
            self.on.update_status,
            self.on.upgrade_charm,
        ):
            self.framework.observe(event, self._reconcile)

    def _reconcile(self, _: ops.EventBase) -> None:
        """Bring the charm to its desired state.

        This is intentionally minimal for now. Provider configuration handling is
        added in follow-up work. With no configuration sources wired up yet, the
        charm is always active.
        """
        logger.debug("Reconciling airflow-provider-configurator charm.")
        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowProviderConfiguratorCharm)
