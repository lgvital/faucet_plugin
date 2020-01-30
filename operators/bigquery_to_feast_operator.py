import grpc


from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator
from faucet_plugin.hooks.feast_hook import FeastHook
from feast import Client, FeatureSet, Entity, ValueType
from google.protobuf.duration_pb2 import Duration


def _bq_to_feast(
        conn_id: str,
        project: str,
        sql: str,
        entity_name: str,
        feature_set: str
):
    feast_hook = FeastHook(conn_id)
    client = feast_hook.get_client(project)
    bq = BigQueryHook(use_legacy_sql=False, location='US')
    features_df = bq.get_pandas_df(sql)
    try:
        # get latest feature set if it exists, otherwise create one
        fs = client.get_feature_set(name=feature_set, project=project)
    except grpc.RpcError as e:
        # TODO: move this logic to operator instead
        fs = FeatureSet(
            feature_set,
            max_age=Duration(seconds=86400),
            # TODO: support other value types / multiple entities
            entities=[Entity(name=entity_name, dtype=ValueType.INT64)]
        )
        # TODO: add a kwarg to pass in explicit feature set dict
        fs.infer_fields_from_df(features_df, replace_existing_features=True)
        client.apply(fs)

    # ingest features into feast (a partitioned BQ table)
    client.ingest(feature_set, features_df)
    # TODO: optimize / add support for ingesting bigquery result in chunks (using faucet?)
    # consider using https://airflow.apache.org/docs/stable/_api/airflow/contrib/hooks/bigquery_hook/index.html#airflow.contrib.hooks.bigquery_hook.BigQueryBaseCursor.run_query


class BigQueryToFeastOperator(PythonOperator):
    def __init__(
            self,
            conn_id: str,
            project: str,
            sql: str,
            entity_name: str,
            feature_set: str,
            *args,
            **kwargs
    ):
        super(BigQueryToFeastOperator, self).__init__(
            python_callable=_bq_to_feast,
            op_args=[conn_id, project, sql, entity_name, feature_set],
            *args,
            **kwargs
        )
