#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Public API of the airflow_provider_configuration relation interface.

This package implements both sides of the `airflow_provider_configuration`
relation between the airflow-provider-configurator charm (provider) and the
airflow-coordinator charm (requirer).
"""

from airflow_provider_configurator.interface import (
    AirflowProviderConfiguratorProviderModel,
    AirflowProviderConfiguratorProvides,
    AirflowProviderConfiguratorRequires,
    SecretNotReadyError,
)

__all__ = [
    "AirflowProviderConfiguratorProviderModel",
    "AirflowProviderConfiguratorProvides",
    "AirflowProviderConfiguratorRequires",
    "SecretNotReadyError",
]
