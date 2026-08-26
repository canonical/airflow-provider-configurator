"""Library to manage the relation provided by the Airflow Provider Configurator charm.

This library contains the Provides (and, later, Requires) classes for handling the
`airflow_provider_configuration` relation between the Airflow Provider Configurator
charm and the Airflow Coordinator charm.

Since the configurator is expected to share sensitive provider data (credentials,
tokens) with the coordinator, it is essential to prevent storing this data in
plaintext in the relation databag. This library abstracts the transparent storage
and retrieval of fields in a pydantic model: plaintext fields live in the databag,
sensitive fields are stored in a Juju secret. We build on the implementation
approach established in data_interfaces v1.

### Provider Charm (this charm)

```python
import charms.airflow_provider_configurator.v0.airflow_provider_configurator as apc

class AirflowProviderConfiguratorCharm(ops.CharmBase):
    def __init__(self, framework):
        super().__init__(framework)
        self.provider = apc.AirflowProviderConfiguratorProvides(self)

    def _reconcile(self, _):
        # After reading/validating provider config from a source (local INI for now):
        self.provider.set_config(
            provider_name="databricks",
            provider_config={
                "databricks": {"host": "https://example.cloud.databricks.com"},
            },
            sensitive_data={"token": "dapi123..."},
        )
        # Or, when there's nothing to share:
        # self.provider.clear_config()
```
"""

import typing

import charms.data_platform_libs.v1.data_interfaces as data_interfaces
import ops
import pydantic

# The unique Charmhub library identifier, never change it.
# NOTE: placeholder. Generate a real one with:
#   charmcraft create-lib airflow_provider_configurator
LIBID = "REPLACE_WITH_GENERATED_LIBID"

# Increment this major API version when introducing breaking changes.
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version.
LIBPATCH = 1

# Dependencies of the library, copied into the charm's own dependencies.
PYDEPS = ["pydantic>=2"]


DEFAULT_RELATION_NAME = "airflow-provider-configuration"


# A secret-backed optional string. Fields annotated this way are excluded from the
# plaintext databag (Field(exclude=True)) and stored in a Juju secret instead,
# grouped under the "sensitive-data" secret group.
SensitiveDataSecretStr = typing.Annotated[
    data_interfaces.OptionalSecretStr,
    pydantic.Field(exclude=True, default=None),
    "sensitive-data",
]


class AirflowProviderConfiguratorProviderModel(data_interfaces.BaseCommonModel):
    """Provider side of the Airflow Provider Configurator relation.

    Attributes:
        provider_name: the name of the Airflow provider this config is for
            (e.g. "databricks", "amazon").
        provider_config: the non-sensitive provider configuration, as a nested
            mapping of INI section to option to value
            (e.g. {"databricks": {"host": "https://..."}}).
        sensitive_data: sensitive values (credentials, tokens) referenced by the
            provider config. Stored in a Juju secret, never written to the databag
            in plaintext.
        secret_sensitive_data: the Juju secret URI backing sensitive_data,
            managed by data_interfaces.
    """

    provider_name: typing.Optional[str] = None
    provider_config: typing.Optional[dict[str, dict[str, str]]] = None
    sensitive_data: SensitiveDataSecretStr = None
    secret_sensitive_data: typing.Optional[data_interfaces.SecretString] = None

    # hack to enable databag diff computation with data_interfaces v1 charm lib
    request_id: str = pydantic.Field(default="fixed_request_id", exclude=True)


class AirflowProviderConfiguratorProvides(ops.Object):
    """Provider handler encapsulating the airflow-provider-configuration relation.

    This is the sending side: the configurator charm uses it to publish provider
    configuration to the related Airflow Coordinator charm.
    """

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str = DEFAULT_RELATION_NAME,
    ):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._relation_name = relation_name

        # data_interfaces repository interface: knows how to write the model to a
        # relation, transparently splitting plaintext (databag) from secret fields.
        self._interface = data_interfaces.OpsRelationRepositoryInterface(
            self._charm.model,
            relation_name,
            AirflowProviderConfiguratorProviderModel,
        )

    def set_config(
        self,
        provider_name: str,
        provider_config: dict[str, dict[str, str]],
        sensitive_data: typing.Optional[dict[str, str]] = None,
    ) -> None:
        """Publish provider configuration to all related coordinator charms.

        Args:
            provider_name: the Airflow provider this config is for.
            provider_config: non-sensitive provider config as a nested mapping of
                INI section to option to value.
            sensitive_data: sensitive values referenced by the config; stored as a
                Juju secret, not in the databag plaintext.
        """
        model = AirflowProviderConfiguratorProviderModel(
            provider_name=provider_name,
            provider_config=provider_config,
            sensitive_data=sensitive_data or None,
        )
        for relation in self._interface.relations:
            self._interface.write_model(relation.id, model)

    def clear_config(self) -> None:
        """Remove any published provider configuration from all relations."""
        empty = AirflowProviderConfiguratorProviderModel()
        for relation in self._interface.relations:
            self._interface.write_model(relation.id, empty)