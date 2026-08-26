#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

r"""Implementation of the `airflow_provider_configuration` relation interface.

This module contains the data model shared over the relation, plus the provider
and requirer handler classes.

The provider (the airflow-provider-configurator charm) shares:
  * `provider_configuration`: a Jinja2 template string for the non-sensitive
    provider configuration, with placeholders (e.g. `{{ gcs__conn_id }}`) where
    sensitive values belong.
  * `provider_configuration_secret_id`: the URI of a charm secret holding the
    sensitive values used to render the template.

The requirer (the airflow-coordinator charm) reads the template, resolves the
secret to obtain the sensitive values, and merges the result into airflow.cfg.

### Provider Charm (airflow-provider-configurator)

```python
from airflow_provider_configurator import AirflowProviderConfigurationProvides

class AirflowProviderConfiguratorCharm(ops.CharmBase):
    def __init__(self, framework):
        super().__init__(framework)
        self.provider = AirflowProviderConfigurationProvides(self)

    def _reconcile(self, _):
        self.provider.set_configuration(
            provider_configuration="[gcs]\\nconn_id = {{ gcs__conn_id }}\\n",
            provider_configuration_sensitive_data={"gcs__conn_id": "secret-value"},
        )
```

### Requirer Charm (airflow-coordinator)

```python
from airflow_provider_configurator import AirflowProviderConfigurationRequires

class AirflowCoordinatorCharm(ops.CharmBase):
    def __init__(self, framework):
        super().__init__(framework)
        self.provider_config = AirflowProviderConfigurationRequires(self)

    def _on_relation_changed(self, event):
        template = self.provider_config.configurations()
        sensitive = self.provider_config.get_sensitive_data()
        keys = self.provider_config.configuration_keys()
```
"""

import typing

import charms.data_platform_libs.v1.data_interfaces as data_interfaces
import ops

DEFAULT_RELATION_NAME = "airflow-provider-configuration"

# Label of the charm secret (distinct from the user-provided secret) that the
# provider creates to hold sensitive data and grants to the requirer.
CHARM_PROVIDER_CONFIG_SECRET_LABEL = "provider-configuration-charm-secret"


class AirflowProviderConfiguratorProviderModel(data_interfaces.BaseCommonModel):
    """Data shared over the airflow_provider_configuration relation (provider side).

    Attributes:
        provider_configuration: a Jinja2 template string for the non-sensitive
            provider configuration, with placeholders for sensitive values.
        provider_configuration_secret_id: the URI of the charm secret holding the
            sensitive values that render `provider_configuration`.
    """

    provider_configuration: typing.Optional[str] = None
    provider_configuration_secret_id: typing.Optional[str] = None


class AirflowProviderConfigurationProvides(ops.Object):
    """Provider side of the airflow_provider_configuration relation.

    Used by the airflow-provider-configurator charm to publish validated provider
    configuration (both the non-sensitive template and a reference to a charm
    secret holding sensitive values) to related airflow-coordinator charms.
    """

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str = DEFAULT_RELATION_NAME,
    ):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._relation_name = relation_name
        self._interface = data_interfaces.OpsRelationRepositoryInterface(
            self._charm.model,
            relation_name,
            AirflowProviderConfiguratorProviderModel,
        )

    def set_configuration(
        self,
        provider_configuration: str,
        provider_configuration_sensitive_data: dict[str, str],
    ) -> None:
        """Publish provider configuration to the related coordinator charm.

        Creates (or updates) a charm secret holding the sensitive data, grants it
        to the related application, and writes the non-sensitive template plus the
        secret id to the relation databag.

        No-op if there is no relation or if this unit is not the leader.

        Args:
            provider_configuration: Jinja2 template string with placeholders for
                sensitive values.
            provider_configuration_sensitive_data: mapping of placeholder name to
                sensitive value, stored in a charm secret.
        """
        if not self._charm.unit.is_leader():
            return
        relations = self._charm.model.relations[self._relation_name]
        if not relations:
            return

        # TODO(verify against ops.Secret + data_interfaces before finalising):
        # 1. Create the charm secret with the sensitive data if it does not exist,
        #    labelled CHARM_PROVIDER_CONFIG_SECRET_LABEL.
        # 2. Otherwise, update the existing secret's content with the new data.
        # 3. Grant the secret to each related application (airflow-coordinator).
        # 4. Build the model with provider_configuration + the secret id and write
        #    it to the databag via self._interface.write_model(relation.id, model).
        raise NotImplementedError("set_configuration body pending secret-API verification")

    def clear_configuration(self) -> None:
        """Remove published provider configuration from all relations.

        Useful when validation fails, when there is no .ini to read after a new
        commit, or when configuration should otherwise be withdrawn.

        No-op if this unit is not the leader.
        """
        if not self._charm.unit.is_leader():
            return

        # TODO(verify): write an empty model to each relation to clear the databag,
        # and remove/clear the charm secret as appropriate.
        raise NotImplementedError("clear_configuration body pending secret-API verification")


class AirflowProviderConfigurationRequires(ops.Object):
    """Requirer side of the airflow_provider_configuration relation.

    Used by the airflow-coordinator charm to read provider configuration shared by
    the airflow-provider-configurator charm.
    """

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str = DEFAULT_RELATION_NAME,
    ):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._relation_name = relation_name
        self._interface = data_interfaces.OpsRelationRepositoryInterface(
            self._charm.model,
            relation_name,
            AirflowProviderConfiguratorProviderModel,
        )

    def configurations(self) -> typing.Optional[str]:
        """Return the non-sensitive provider configuration template.

        Reads the `provider_configuration` field (a Jinja2 template string) from
        the relation databag. Returns None if there is no relation or no data yet.
        """
        # TODO(verify): read the model with
        #   self._interface.build_model(relation.id, ..., component=relation.app)
        # and return model.provider_configuration.
        raise NotImplementedError("configurations body pending verification")

    def get_sensitive_data(self) -> dict[str, str]:
        """Return the sensitive values held in the provider's charm secret.

        Resolves `provider_configuration_secret_id` to the charm secret and reads
        its content, returning a mapping of placeholder name to value.
        """
        # TODO(verify): resolve provider_configuration_secret_id via
        # self._charm.model.get_secret(id=...).get_content() and return it.
        raise NotImplementedError("get_sensitive_data body pending verification")

    def configuration_keys(self) -> set[str]:
        """Return the set of section.option keys the provider would set.

        Lets the coordinator detect collisions between provider-supplied
        configuration and the configuration it sets itself (Layer 1 validation).
        """
        # TODO(verify): parse the provider_configuration template into
        # section.option keys and return them as a set.
        raise NotImplementedError("configuration_keys body pending verification")

