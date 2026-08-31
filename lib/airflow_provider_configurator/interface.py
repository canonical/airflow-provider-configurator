#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

r"""Implementation of the `airflow_provider_configuration` relation interface.

This module contains the data model shared over the relation, plus the provider
and requirer handler classes. It is implemented directly on top of `ops`, with no
external charm-lib dependency.

The provider (the airflow-provider-configurator charm) shares:
  * `provider_configuration`: a Jinja2 template string for the non-sensitive
    provider configuration, with placeholders (e.g. `{{ gcs__conn_id }}`) where
    sensitive values belong.
  * `provider_configuration_secret_uri`: the URI of a charm secret holding the
    sensitive values used to render the template.

The requirer (the airflow-coordinator charm) reads the template, resolves the
secret to obtain the sensitive values, and merges the result into airflow.cfg.

### Provider Charm (airflow-provider-configurator)

```python
from airflow_provider_configurator import AirflowProviderConfiguratorProvides

class AirflowProviderConfiguratorCharm(ops.CharmBase):
    def __init__(self, framework):
        super().__init__(framework)
        self.provider = AirflowProviderConfiguratorProvides(self)

    def _reconcile(self, _):
        self.provider.set_configuration(
            provider_configuration="[gcs]\nconn_id = {{ gcs__conn_id }}\n",
            provider_configuration_sensitive_data={"gcs__conn_id": "secret-value"},
        )
```

### Requirer Charm (airflow-coordinator)

```python
from airflow_provider_configurator import AirflowProviderConfiguratorRequires

class AirflowCoordinatorCharm(ops.CharmBase):
    def __init__(self, framework):
        super().__init__(framework)
        self.provider_config = AirflowProviderConfiguratorRequires(self)

    def _on_relation_changed(self, event):
        template = self.provider_config.configurations()
        sensitive = self.provider_config.get_sensitive_data()
        keys = self.provider_config.configuration_keys()
```
"""

import configparser
import json
from dataclasses import dataclass

import ops

DEFAULT_RELATION_NAME = "airflow-provider-configuration"

# Label of the charm secret (distinct from the user-provided secret) that the
# provider creates to hold sensitive data and grants to the requirer.
CHARM_PROVIDER_CONFIG_SECRET_LABEL = "provider-configuration-charm-secret"

# Juju secret keys must be lowercase alphanumeric and cannot contain the
# double-underscore placeholder names (e.g. "gcs__conn_id"). The sensitive data
# map is therefore JSON-encoded and stored under this single valid key.
SENSITIVE_DATA_SECRET_KEY = "sensitive-data"

# Relation databag keys (hyphenated, per convention).
DATABAG_KEY_CONFIGURATION = "provider-configuration"
DATABAG_KEY_SECRET_URI = "provider-configuration-secret-uri"


@dataclass
class AirflowProviderConfiguratorProviderModel:
    """Data shared over the airflow_provider_configuration relation (provider side).

    Attributes:
        provider_configuration: a Jinja2 template string for the non-sensitive
            provider configuration, with placeholders for sensitive values.
        provider_configuration_secret_uri: the URI of the charm secret holding the
            sensitive values that render `provider_configuration`.
    """

    provider_configuration: str | None = None
    provider_configuration_secret_uri: str | None = None


class AirflowProviderConfiguratorProvides(ops.Object):
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

    def _set_secret(
        self,
        content: dict[str, str],
        relations: list[ops.Relation],
    ) -> ops.Secret:
        """Create or update the charm secret holding the sensitive data.

        Grants the secret to every related application. Returns the Secret so the
        caller can read its id for the databag.
        """
        secret_content = {SENSITIVE_DATA_SECRET_KEY: json.dumps(content)}
        try:
            secret = self._charm.model.get_secret(label=CHARM_PROVIDER_CONFIG_SECRET_LABEL)
            # Only write a new revision if the content actually changed, otherwise a
            # reconcile-driven charm would churn secret revisions on every hook.
            if secret.get_content(refresh=True) != secret_content:
                secret.set_content(secret_content)
        except ops.SecretNotFoundError:
            secret = self._charm.app.add_secret(
                secret_content, label=CHARM_PROVIDER_CONFIG_SECRET_LABEL
            )
        for relation in relations:
            secret.grant(relation)
        return secret

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

        secret = self._set_secret(provider_configuration_sensitive_data, relations)
        if not secret.id:
            raise RuntimeError("Charm secret has no id; cannot publish to the databag.")

        for relation in relations:
            databag = relation.data[self._charm.app]
            databag[DATABAG_KEY_CONFIGURATION] = provider_configuration
            databag[DATABAG_KEY_SECRET_URI] = secret.id


class AirflowProviderConfiguratorRequires(ops.Object):
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

    def _get_model(self) -> AirflowProviderConfiguratorProviderModel | None:
        """Read the provider model from the relation databag."""
        relation = self._charm.model.get_relation(self._relation_name)
        if not relation or not relation.app:
            return None
        databag = relation.data[relation.app]
        configuration = databag.get(DATABAG_KEY_CONFIGURATION)
        secret_uri = databag.get(DATABAG_KEY_SECRET_URI)
        if configuration is None and secret_uri is None:
            return None
        return AirflowProviderConfiguratorProviderModel(
            provider_configuration=configuration,
            provider_configuration_secret_uri=secret_uri,
        )

    def configurations(self) -> str | None:
        """Return the non-sensitive provider configuration template.

        Reads the `provider-configuration` field (a Jinja2 template string) from
        the relation databag. Returns None if there is no relation or no data yet.
        """
        model = self._get_model()
        return model.provider_configuration if model else None

    def get_sensitive_data(self) -> dict[str, str]:
        """Return the sensitive values held in the provider's charm secret.

        Resolves `provider_configuration_secret_uri` to the charm secret and reads
        its content, returning a mapping of placeholder name to value.
        """
        model = self._get_model()
        if not model or not model.provider_configuration_secret_uri:
            return {}
        secret = self._charm.model.get_secret(id=model.provider_configuration_secret_uri)
        content = secret.get_content(refresh=True)
        return json.loads(content[SENSITIVE_DATA_SECRET_KEY])

    def configuration_keys(self) -> set[str]:
        """Return the set of section.option keys the provider would set.

        Lets the coordinator detect collisions between provider-supplied
        configuration and the configuration it sets itself (Layer 1 validation).
        """
        model = self._get_model()
        if not model or not model.provider_configuration:
            return set()
        # Match the generator's parser exactly (RawConfigParser + case-preserving
        # optionxform): the default ConfigParser lowercases options and applies %
        # interpolation, which would both corrupt collision detection.
        parser = configparser.RawConfigParser()
        parser.optionxform = str  # type: ignore[assignment, method-assign]
        parser.read_string(model.provider_configuration)
        return {
            f"{section}.{option}"
            for section in parser.sections()
            for option in parser.options(section)
        }
