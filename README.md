# Airflow Plugin - Faucet

This plugin has helper hooks and operators for your feast feature store.

## Installation
### Install latest feast client onto your airflow cluster

E.g. if you're using Google Cloud Composer, [install feast via PyPi](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies#install-package).

If you run into issues related to conflicting dependencies, e.g.

```
ImportError: cannot import name 'client_pb2' from 'google.api'
```

Also upgrade `googleapis-common-protos`: `googleapis-common-protos>=1.51.0`.

## Hooks

### FeastHook
This hook handles connection to your feast feature store..

## Operators

### BigQueryToFeastOperator
This operator ingests data from a bigquery sql query into a specific project and feature set in feast.

### BigQueryToFeastFeatureSetOperator
Create or update a feature set by inferring spec from a BigQuery SQL query sample.

### FeastFeatureSetOperator
Create or update a feature set from an explicit spec via YAML or a dict.
