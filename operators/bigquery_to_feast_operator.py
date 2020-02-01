from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator
from faucet_plugin.hooks.feast_hook import FeastHook


def _bq_to_feast(
        conn_id: str,
        project: str,
        sql: str,
        feature_set: str
):
    feast_hook = FeastHook(conn_id)
    client = feast_hook.get_client(project)
    bq = BigQueryHook(use_legacy_sql=False, location='US')
    features_df = bq.get_pandas_df(sql)
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
            feature_set: str,
            *args,
            **kwargs
    ):
        super(BigQueryToFeastOperator, self).__init__(
            python_callable=_bq_to_feast,
            op_args=[conn_id, project, sql, feature_set],
            *args,
            **kwargs
        )
