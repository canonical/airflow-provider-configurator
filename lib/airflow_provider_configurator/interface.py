#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

r"""Implementation of the `airflow_provider_configuration` relation interface.

This module contains the data model shared over the relation, plus the provider
and requirer handler classes. It is implemented directly on top of `ops`, with no
external charm-lib dependency.

The provider (the airflow-provider-configurator charm) shares:
  * `provider_configuration`: a Jinja2 template string for the non-sensitive
    provider configuration, with placeholders (e.g. `{{ provider__gcs__conn_id }}`)
    where sensitive values belong.
  * `provider_configuration_secret_uri`: the URI of a charm secret holding the
    sensitive values used to render the template.

The requirer (the airflow-coordinator charm) reads the template and resolves the
secret to obtain the sensitive values. The actual render into airflow.cfg happens
later, once, in whichever charm (a core charm, or the coordinator itself for its
own DB-migration copy) writes the file.

### Provider Charm (airflow-provider-configurator)

```python
from airflow_provider_configurator import AirflowProviderConfiguratorProvides

class AirflowProviderConfiguratorCharm(ops.CharmBase):
    def __init__(self, framework):
        super().__init__(framework)
        self.provider = AirflowProviderConfiguratorProvides(self)

    def _reconcile(self, _):
        self.provider.set_configuration(
            provider_configuration="[gcs]\nconn_id = {{ provider__gcs__conn_id }}\n",
            provider_configuration_sensitive_data={"provider__gcs__conn_id": "secret-value"},
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
# double-underscore placeholder names (e.g. "provider__gcs__conn_id"). The
# sensitive data map is therefore JSON-encoded and stored under this single valid key.
SENSITIVE_DATA_SECRET_KEY = "sensitive-data"

# Relation databag keys (hyphenated, per convention).
DATABAG_KEY_CONFIGURATION = "provider-configuration"
DATABAG_KEY_SECRET_URI = "provider-configuration-secret-uri"


class SecretNotReadyError(Exception):
    """Raised when the provider's charm secret is not accessible yet.

    Typically because the secret has not been granted to this charm, or the grant
    has not propagated. The charm should catch this and set a blocked status
    rather than letting an unhandled exception surface.
    """


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

    def _get_or_create_secret(self, content: dict[str, str]) -> ops.Secret:
        """Return the charm secret holding the sensitive data, creating it if needed.

        Args:
            content: a FLAT mapping of Jinja2 placeholder name (e.g.
                "provider__gcs__conn_id", matching a ``{{ provider__gcs__conn_id }}``
                in the template) to its sensitive value. Used to seed the secret if
                it does not yet exist. Stored JSON-encoded under a single
                Juju-valid secret key.

        Returns:
            The existing or newly created charm secret.
        """
        try:
            return self._charm.model.get_secret(label=CHARM_PROVIDER_CONFIG_SECRET_LABEL)
        except ops.SecretNotFoundError:
            secret_content = {SENSITIVE_DATA_SECRET_KEY: json.dumps(content)}
            return self._charm.app.add_secret(
                secret_content, label=CHARM_PROVIDER_CONFIG_SECRET_LABEL
            )

    def _set_secret(
        self,
        secret: ops.Secret,
        content: dict[str, str],
        relations: list[ops.Relation],
    ) -> None:
        """Update the charm secret's content if it changed, and grant it.

        Args:
            secret: the charm secret to update and grant. Resolved by the caller
                so it owns the reference used to publish the secret id.
            content: a FLAT mapping of Jinja2 placeholder name (e.g.
                "provider__gcs__conn_id", matching a ``{{ provider__gcs__conn_id }}``
                in the template) to its sensitive value. Stored JSON-encoded under
                a single Juju-valid secret key.
            relations: the relations whose applications the secret is granted to.
        """
        # Only write a new revision when the sensitive data actually changed,
        # otherwise a reconcile-driven charm would churn secret revisions on
        # every hook. Compare decoded dicts, not the JSON-encoded strings, to
        # avoid false diffs from key ordering.
        current = secret.get_content(refresh=True).get(SENSITIVE_DATA_SECRET_KEY)
        current_content = json.loads(current) if current else None
        if current_content != content:
            secret.set_content({SENSITIVE_DATA_SECRET_KEY: json.dumps(content)})
        for relation in relations:
            secret.grant(relation)

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
            provider_configuration_sensitive_data: a FLAT mapping of placeholder
                name to sensitive value, stored in the charm secret.
        """
        if not self._charm.unit.is_leader():
            return
        relations = self._charm.model.relations[self._relation_name]
        if not relations:
            return

        # Resolve the charm secret once here so this method owns the reference,
        # then thread it through _set_secret (which only updates and grants it).
        secret = self._get_or_create_secret(provider_configuration_sensitive_data)
        self._set_secret(secret, provider_configuration_sensitive_data, relations)
        if not secret.id:
            raise RuntimeError("Charm secret is missing an id; cannot publish to the databag.")

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

        Raises:
            SecretNotReadyError: if the secret is not accessible yet, e.g. it has
                not been granted to this charm. Callers should catch this and set
                a blocked status.
        """
        model = self._get_model()
        if not model or not model.provider_configuration_secret_uri:
            return {}
        try:
            secret = self._charm.model.get_secret(id=model.provider_configuration_secret_uri)
            content = secret.get_content(refresh=True)
        except ops.SecretNotFoundError as e:
            raise SecretNotReadyError(
                "Provider configuration secret is not accessible; "
                "it may not be granted to this charm yet."
            ) from e
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
