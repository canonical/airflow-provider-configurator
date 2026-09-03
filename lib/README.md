# airflow-provider-configurator-charmlib

Common charm library for the provider and requirer sides of the
`airflow_provider_configuration` relation interface.

This interface connects the `airflow-provider-configurator` charm (provider) to
the `airflow-coordinator` charm (requirer). The provider shares a non-sensitive
Jinja2 configuration template together with a reference to a charm secret holding
the sensitive values. The actual render into `airflow.cfg` happens later, once,
in whichever charm (a core charm, or the coordinator itself for its own
DB-migration copy) writes the file.

The source for this package lives inside the
[`airflow-provider-configurator`](https://github.com/canonical/airflow-provider-configurator)
repository, under `lib/`.

## Usage

Provider (airflow-provider-configurator):

```python
from airflow_provider_configurator import AirflowProviderConfiguratorProvides
```

Requirer (airflow-coordinator):

```python
from airflow_provider_configurator import AirflowProviderConfiguratorRequires
```

See `airflow_provider_configurator/interface.py` for the full API.